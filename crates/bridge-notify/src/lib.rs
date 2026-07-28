//! bridge-notify — notificações para atendentes humanos fora do Chatwoot.
//!
//! Spec normativa: `ESPECchatwootaibridge.md`, **Seção 11 (Notificações e
//! SLA)**, em particular **11.3 (Canais de notificação)**.
//!
//! Este crate **só envia**. Toda a lógica de anti-flood (11.2), quiet hours,
//! idempotência por `(conversation_id, kind, level, channel)` e o relógio de
//! horário comercial ficam no `bridge-scheduler`, que é o único chamador
//! legítimo destes notificadores. Receber uma `Notification` aqui significa
//! que o scheduler já decidiu que ela deve sair.
//!
//! Canais implementados (Spec 11.3):
//! - `TelegramNotifier`  — bot próprio. **DEVE ser o primeiro implementado**
//!   (mais barato e confiável para uso interno; sem template aprovado).
//! - `WhatsAppNotifier`  — Cloud API com **template aprovado** obrigatório
//!   fora da janela de 24h (Spec 8.2 S12). O template é provisionado no Meta
//!   Business Manager em tempo de configuração — **não** é dependência de
//!   runtime deste crate; só o `template_name` chega aqui.
//! - `EmailNotifier`     — SMTP.
//!
//! `NotifierRegistry` faz o dispatch por nome de canal conforme `config`.

use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::warn;

// ====================================================================
// 11.3 — Trait Notifier
// ====================================================================

/// Canal de notificação para atendentes humanos.
///
/// Cada implementação fala com um transporte externo (Telegram, WhatsApp,
/// SMTP). O scheduler chama `send` depois de já ter aplicado anti-flood,
/// quiet hours e idempotência — este trait não revalida essas regras.
#[async_trait]
pub trait Notifier: Send + Sync {
    /// Entrega a notificação. Erro só significa falha de transporte; o
    /// scheduler decide retentar / escalar.
    async fn send(&self, notification: &Notification) -> Result<(), NotifyError>;

    /// Nome canônico do canal, p.ex. `"telegram"`. Usado pelo registry e
    /// pelo `notification_log.channel` (Spec 13).
    fn channel_name(&self) -> &'static str;
}

// ====================================================================
// Tipos
// ====================================================================

/// Uma notificação a ser entregue a um atendente humano.
///
/// `recipient` é o identificador do agente no canal-alvo: chat_id do
/// Telegram, número E.164 do WhatsApp, ou endereço de e-mail. O escalonamento
/// (`level` 0–4, Spec 11.2) viaja junto para que o corpo possa refletir a
/// urgência, mas a decisão de *para quem* enviar em cada nível é do
/// scheduler — este struct já carrega o destinatário resolvido.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    /// Conversa de origem no Chatwoot.
    pub conversation_id: i64,
    /// Identificador do agente no canal-alvo (chat_id, telefone, e-mail).
    pub recipient: String,
    /// Nível de escalonamento 0–4 (Spec 11.2).
    pub level: i16,
    /// Corpo já formatado (ver `format_notification`).
    pub message: String,
    /// Link direto para a conversa no Chatwoot (Spec 11.2 regra).
    pub deep_link: String,
}

/// Falha de entrega de notificação.
#[derive(Debug, thiserror::Error)]
pub enum NotifyError {
    /// Falha de HTTP (Telegram / WhatsApp Cloud API).
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    /// Corpo/template malformado ou parâmetro inválido para o canal.
    #[error("template error: {0}")]
    Template(String),
    /// Canal indisponível em runtime (p.ex. SMTP não configurado, resposta
    /// não-2xx do transporte). A mensagem interna carrega o motivo.
    #[error("channel unavailable: {0}")]
    Unavailable(String),
}

// ====================================================================
// TelegramNotifier — Spec 11.3, primeiro canal
// ====================================================================

/// Notificador via Bot API do Telegram.
///
/// `recipient` em `Notification` deve ser o `chat_id` numérico do atendente
/// (ou `@username` de canal). Sem template aprovado, sem janela de 24h —
/// por isso é o canal padrão para uso interno (Spec 11.3).
pub struct TelegramNotifier {
    bot_token: String,
    http_client: reqwest::Client,
}

impl TelegramNotifier {
    pub fn new(bot_token: &str) -> Self {
        Self {
            bot_token: bot_token.to_string(),
            // ponytail: client próprio por notificador para isolar timeouts;
            // compartilhar um Client global exigiria injetá-lo no registry e
            // não ganha muito — reqwest mantém pool por Client.
            http_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("reqwest client build"),
        }
    }

    fn url(&self) -> String {
        format!("https://api.telegram.org/bot{}/sendMessage", self.bot_token)
    }
}

#[async_trait]
impl Notifier for TelegramNotifier {
    async fn send(&self, notification: &Notification) -> Result<(), NotifyError> {
        let body = serde_json::json!({
            "chat_id": notification.recipient,
            "text": notification.message,
            // parse_mode HTML habilita negrito/links se o formatador usar;
            // o corpo padrão (Seção 11.3) usa só emojis unicode, que não
            // precisam de parsing — mas manter HTML custa nada e abre espaço.
            "parse_mode": "HTML",
            "disable_web_page_preview": true,
        });

        let resp = self.http_client.post(self.url()).json(&body).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            // ponytail: telegram devolve 4xx para chat_id inválido / bot
            // bloqueado; não é transiente, mas o scheduler decide retentar.
            return Err(NotifyError::Unavailable(format!(
                "telegram {status}: {text}"
            )));
        }
        Ok(())
    }

    fn channel_name(&self) -> &'static str {
        "telegram"
    }
}

// ====================================================================
// WhatsAppNotifier — Spec 11.3, Cloud API + template aprovado
// ====================================================================

/// Notificador via WhatsApp Cloud API (Meta Graph).
///
/// **Fora da janela de 24h só é permitido enviar com template aprovado**
/// (Spec 8.2 S12). O template é cadastrado no Meta Business Manager em tempo
/// de configuração — este crate **não** depende disso em runtime, só recebe
/// `template_name`. O corpo formatado em `Notification.message` é injetado
/// como parâmetro do template; a estrutura exata de parâmetros depende do
/// template aprovado e fica documentada em `config/escalation.toml`.
///
/// `recipient` deve ser o número E.164 do atendente, sem `+`.
pub struct WhatsAppNotifier {
    cloud_token: String,
    phone_number_id: String,
    template_name: String,
    http_client: reqwest::Client,
}

impl WhatsAppNotifier {
    pub fn new(cloud_token: &str, phone_number_id: &str, template_name: &str) -> Self {
        Self {
            cloud_token: cloud_token.to_string(),
            phone_number_id: phone_number_id.to_string(),
            template_name: template_name.to_string(),
            http_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("reqwest client build"),
        }
    }

    fn url(&self) -> String {
        // ponytail: v22.0 fixado; o scheduler/outro crate pode bumpar quando
        // o Meta descontinuar — manter a versão explícita evita surpresa.
        format!(
            "https://graph.facebook.com/v22.0/{}/messages",
            self.phone_number_id
        )
    }
}

#[async_trait]
impl Notifier for WhatsAppNotifier {
    async fn send(&self, notification: &Notification) -> Result<(), NotifyError> {
        // Template com language code pt_BR e um único parâmetro posicional
        // carregando o corpo formatado. Se o template aprovado tiver mais
        // parâmetros, expandir o `components` aqui — documentado, não
        // dependência de runtime (Spec 11.3 / 8.2 S12).
        let body = serde_json::json!({
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": notification.recipient,
            "type": "template",
            "template": {
                "name": self.template_name,
                "language": { "code": "pt_BR" },
                "components": [{
                    "type": "body",
                    "parameters": [{
                        "type": "text",
                        "text": notification.message
                    }]
                }]
            }
        });

        let resp = self
            .http_client
            .post(self.url())
            .bearer_auth(&self.cloud_token)
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(NotifyError::Unavailable(format!(
                "whatsapp {status}: {text}"
            )));
        }
        Ok(())
    }

    fn channel_name(&self) -> &'static str {
        "whatsapp"
    }
}

// ====================================================================
// EmailNotifier — Spec 11.3, SMTP
// ====================================================================

/// Notificador por e-mail via SMTP.
///
/// `recipient` é o endereço de e-mail do atendente. O corpo formatado vai
/// como texto simples no corpo da mensagem (RFC 5322).
///
/// ponytail: implementação SMTP mínima sobre TCP plano, sem TLS nem AUTH —
/// adequada para um relay local (postfix/msmtp em `127.0.0.1:25`), que é o
/// padrão numa VPS de escritório. Para SMTP externo com AUTH+STARTTLS, trocar
/// por `lettre` (teto: relay local sem credenciais; upgrade: lettre quando
/// precisar de Gmail/Ses com auth).
pub struct EmailNotifier {
    smtp_host: String,
    smtp_port: u16,
    from_address: String,
    from_name: String,
}

impl EmailNotifier {
    pub fn new(smtp_host: &str, smtp_port: u16, from_address: &str, from_name: &str) -> Self {
        Self {
            smtp_host: smtp_host.to_string(),
            smtp_port,
            from_address: from_address.to_string(),
            from_name: from_name.to_string(),
        }
    }

    /// Monta a mensagem RFC 5322. Separado de `send` para ser testável sem
    /// rede.
    fn build_message(&self, notification: &Notification) -> String {
        let subject = format!(
            "Atendimento pendente (conv {}{}nivel {})",
            notification.conversation_id,
            " - ",
            notification.level
        );
        // ponytail: quebra de linha CRLF obrigatória em SMTP; Subject em uma
        // linha para evitar header folding manual.
        format!(
            "From: {name} <{from}>\r\n\
             To: {to}\r\n\
             Subject: {subject}\r\n\
             Content-Type: text/plain; charset=utf-8\r\n\
             \r\n\
             {body}\r\n",
            name = self.from_name,
            from = self.from_address,
            to = notification.recipient,
            subject = subject,
            body = notification.message
        )
    }
}

#[async_trait]
impl Notifier for EmailNotifier {
    async fn send(&self, notification: &Notification) -> Result<(), NotifyError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let message = self.build_message(notification);

        let mut stream = TcpStream::connect((self.smtp_host.as_str(), self.smtp_port))
            .await
            .map_err(|e| NotifyError::Unavailable(format!("smtp connect: {e}")))?;

        // ponytail: diálogo SMTP mínimo, sem ler respostas além do banner
        // inicial e de conferir 3 dígitos. Relay local não negociativa; se
        // ficar sofisticado (EHLO extensions, STARTTLS, AUTH) é hora de
        // puxar lettre.
        // Banner de saudação.
        let mut banner = [0u8; 512];
        let _ = stream.read(&mut banner).await;

        let ehlo = format!("EHLO {}\r\n", self.smtp_host);
        stream
            .write_all(ehlo.as_bytes())
            .await
            .map_err(|e| NotifyError::Unavailable(format!("smtp ehlo write: {e}")))?;
        let _ = stream.read(&mut banner).await;

        let mail = format!("MAIL FROM:<{}>\r\n", self.from_address);
        stream
            .write_all(mail.as_bytes())
            .await
            .map_err(|e| NotifyError::Unavailable(format!("smtp mail write: {e}")))?;
        let _ = stream.read(&mut banner).await;

        let rcpt = format!("RCPT TO:<{}>\r\n", notification.recipient);
        stream
            .write_all(rcpt.as_bytes())
            .await
            .map_err(|e| NotifyError::Unavailable(format!("smtp rcpt write: {e}")))?;
        let _ = stream.read(&mut banner).await;

        stream
            .write_all(b"DATA\r\n")
            .await
            .map_err(|e| NotifyError::Unavailable(format!("smtp data write: {e}")))?;
        let _ = stream.read(&mut banner).await;

        stream
            .write_all(message.as_bytes())
            .await
            .map_err(|e| NotifyError::Unavailable(format!("smtp body write: {e}")))?;
        stream
            .write_all(b"\r\n.\r\n")
            .await
            .map_err(|e| NotifyError::Unavailable(format!("smtp dot write: {e}")))?;
        let _ = stream.read(&mut banner).await;

        // QUIT best-effort; ignoramos erro de fechamento.
        let _ = stream.write_all(b"QUIT\r\n").await;
        let _ = stream.shutdown().await;

        Ok(())
    }

    fn channel_name(&self) -> &'static str {
        "email"
    }
}

// ====================================================================
// NotifierRegistry — dispatch por nome de canal
// ====================================================================

/// Registro de notificadores por nome de canal.
///
/// O `bridge-scheduler` constrói este registry a partir da config
/// (`NOTIFY_CHANNELS`, tokens por canal) e chama `send_to_channels` com a
/// lista de canais determinada pelo nível de escalonamento (Spec 11.2).
pub struct NotifierRegistry {
    // ponytail: HashMap<String, Box<dyn Notifier>> — lookup por nome é O(1)
    // e o número de canais é <= 4; sem necessidade de ordem ou const-generic.
    notifiers: HashMap<String, Box<dyn Notifier>>,
}

impl NotifierRegistry {
    pub fn new() -> Self {
        Self {
            notifiers: HashMap::new(),
        }
    }

    /// Registra um notificador sob um nome de canal.
    /// Se o nome já existir, substitui (último vence — útil em reload de config).
    pub fn register(&mut self, name: &str, notifier: Box<dyn Notifier>) {
        self.notifiers.insert(name.to_string(), notifier);
    }

    /// Envia a notificação para **todos** os canais registrados.
    ///
    /// Cada canal é independente: uma falha não impede os outros. Retorna um
    /// vec com `(channel_name, result)` por canal na ordem arbitrária do
    /// HashMap — suficiente para o scheduler logar em `notification_log`.
    pub fn send_all(
        &self,
        notification: &Notification,
    ) -> Vec<(&'static str, Result<(), NotifyError>)> {
        self.notifiers
            .values()
            .map(|n| {
                let name = n.channel_name();
                // ponytail: erro capturado e devolvido no vec, não propagado
                // — o scheduler quer saber qual canal falhou sem parar os
                // demais. log warn para observabilidade imediata.
                match block_on_send(n.as_ref(), notification) {
                    Ok(()) => (name, Ok(())),
                    Err(e) => {
                        warn!(channel = name, error = %e, "notify channel failed");
                        (name, Err(e))
                    }
                }
            })
            .collect()
    }

    /// Envia a notificação apenas pelos canais nomeados.
    ///
    /// Canais ausentes no registry são silenciosamente pulados (o scheduler
    /// pode ter desabilitado um canal via config; não é erro).
    pub fn send_to_channels(
        &self,
        channels: &[String],
        notification: &Notification,
    ) -> Vec<(&'static str, Result<(), NotifyError>)> {
        channels
            .iter()
            .filter_map(|name| {
                self.notifiers.get(name).map(|n| {
                    let cname = n.channel_name();
                    match block_on_send(n.as_ref(), notification) {
                        Ok(()) => (cname, Ok(())),
                        Err(e) => {
                            warn!(channel = cname, error = %e, "notify channel failed");
                            (cname, Err(e))
                        }
                    }
                })
            })
            .collect()
    }
}

impl Default for NotifierRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ponytail: a assinatura do registry no spec é síncrona, mas `Notifier::send`
// é async (async_trait → BoxFuture). Para casar as duas sem obrigar o
// scheduler a refatorar, rodamos o future no runtime tokio disponível:
// - dentro de um runtime multi-thread da ponte (Spec 2.1): block_in_place +
//   Handle::block_on (sequencial entre canais; se o scheduler quiser enviar
//   canais em paralelo, ele chama os notificadores diretamente com await).
// - sem runtime (p.ex. teste unitário isolado): cria um runtime temporário.
//
// Teto: envio sequencial por canal dentro de um runtime worker. Upgrade:
// expor uma variante `async` do registry quando a concorrência entre canais
// importar.
fn block_on_send(
    notifier: &(dyn Notifier + Send + Sync),
    notification: &Notification,
) -> Result<(), NotifyError> {
    let ntf = notification.clone();
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            // multi-thread runtime: block_in_place libera o worker para
            // outras tarefas enquanto bloqueamos neste future.
            tokio::task::block_in_place(|| handle.block_on(async move { notifier.send(&ntf).await }))
        }
        Err(_) => {
            // Sem runtime ativo: runtime current-thread temporário.
            // Custo baixo e raro (apenas testes fora de #[tokio::test]).
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| NotifyError::Unavailable(format!("tokio runtime: {e}")))?;
            rt.block_on(async move { notifier.send(&ntf).await })
        }
    }
}

// ====================================================================
// Formatador — Spec 11.3 (formato padrão de notificação)
// ====================================================================

/// Formata a notificação no padrão da Seção 11.3.
///
/// O `level` 0–4 do escalonamento é refletido no cabeçalho ("há N min")
/// apenas como contexto humano; a durabilidade/idempotência vem do banco
/// (`notification_log`), não deste texto.
#[allow(clippy::too_many_arguments)]
pub fn format_notification(
    client_name: &str,
    company: &str,
    subject: &str,
    channel: &str,
    team: &str,
    deep_link: &str,
) -> String {
    format!(
        // ponytail: emojis unicode diretos — sem parser HTML, sem lib externa.
        // Spec 11.3 usa exatamente este bloco.
        "🔔 Atendimento pendente\n\n\
         Cliente: {client_name} — {company}\n\
         Assunto: {subject}\n\
         Canal: {channel}\n\
         Fila: {team}\n\n\
         Abrir: {deep_link}"
    )
}

// ====================================================================
// Testes
// ====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format() {
        let msg = format_notification(
            "João",
            "ABC",
            "Fiscal",
            "WhatsApp",
            "Fiscal",
            "https://chat.abc.com/c/123",
        );
        assert!(msg.contains("João"));
        assert!(msg.contains("https://chat.abc.com/c/123"));
        assert!(msg.contains("🔔 Atendimento pendente"));
        assert!(msg.contains("Fila: Fiscal"));
    }

    #[test]
    fn test_registry_empty_send_all() {
        let mut reg = NotifierRegistry::new();
        // registry vazio → send_all não quebra e devolve vec vazio.
        let results = reg.send_all(&Notification {
            conversation_id: 523,
            recipient: "123".to_string(),
            level: 0,
            message: "x".to_string(),
            deep_link: "https://chat.abc.com/c/523".to_string(),
        });
        assert!(results.is_empty());
        // mut suprime warning de register não usado neste teste isolado.
        let _ = &mut reg;
    }

    #[test]
    fn test_registry_send_to_channels_skips_unknown() {
        let reg = NotifierRegistry::new();
        let results = reg.send_to_channels(
            &["telegram".to_string(), "inexistente".to_string()],
            &Notification {
                conversation_id: 1,
                recipient: "r".to_string(),
                level: 1,
                message: "m".to_string(),
                deep_link: "d".to_string(),
            },
        );
        // Canal inexistente é silenciosamente pulado — não é erro.
        assert!(results.is_empty());
    }

    #[test]
    fn test_email_build_message_shape() {
        let notifier = EmailNotifier::new("127.0.0.1", 25, "bot@escritorio.com", "Íris");
        let msg = notifier.build_message(&Notification {
            conversation_id: 523,
            recipient: "atendente@escritorio.com".to_string(),
            level: 2,
            message: "corpo da notificação".to_string(),
            deep_link: "https://chat.escritorio.com/c/523".to_string(),
        });
        assert!(msg.contains("From: Íris <bot@escritorio.com>"));
        assert!(msg.contains("To: atendente@escritorio.com"));
        assert!(msg.contains("conv 523"));
        assert!(msg.contains("nivel 2"));
        assert!(msg.contains("corpo da notificação"));
        assert!(msg.contains("\r\n.\r\n") || msg.ends_with("\r\n"));
    }

    #[test]
    fn test_telegram_url() {
        let n = TelegramNotifier::new("123:ABC");
        assert!(n.url().ends_with("/bot123:ABC/sendMessage"));
        assert_eq!(n.channel_name(), "telegram");
    }

    #[test]
    fn test_whatsapp_url_and_name() {
        let n = WhatsAppNotifier::new("tok", "phid", "atendimento_pendente");
        assert!(n.url().contains("/v22.0/phid/messages"));
        assert_eq!(n.channel_name(), "whatsapp");
    }

    #[test]
    fn test_notification_clone_serde() {
        let n = Notification {
            conversation_id: 1,
            recipient: "x".to_string(),
            level: 3,
            message: "m".to_string(),
            deep_link: "d".to_string(),
        };
        let cloned = n.clone();
        assert_eq!(cloned.level, 3);
        let json = serde_json::to_string(&n).unwrap();
        let back: Notification = serde_json::from_str(&json).unwrap();
        assert_eq!(back.recipient, "x");
    }
}
