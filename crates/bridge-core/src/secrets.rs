use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

    /// Wrapper de string sensível. `Display` redige — nunca logar o conteúdo.
    /// ponytail: newtype caseiro em vez do crate `secrecy` (não está no workspace).
    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct SecretString(String);

    impl SecretString {
        pub fn new(s: impl Into<String>) -> Self {
            Self(s.into())
        }
        /// Acesso ao conteúdo bruto — só em pontos de uso (client HTTP, HMAC).
        pub fn expose(&self) -> &str {
            &self.0
        }
        pub fn is_empty(&self) -> bool {
            self.0.is_empty()
        }
    }

    impl From<String> for SecretString {
        fn from(s: String) -> Self {
            Self(s)
        }
    }

    impl std::str::FromStr for SecretString {
        type Err = std::convert::Infallible;
        fn from_str(s: &str) -> Result<Self, Self::Err> {
            Ok(Self(s.to_string()))
        }
    }

    impl fmt::Display for SecretString {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("[REDACTED]")
        }
    }

    impl Serialize for SecretString {
        fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            s.serialize_str(&self.0)
        }
    }

    impl<'de> Deserialize<'de> for SecretString {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            let s = String::deserialize(d)?;
            Ok(Self(s))
        }
    }

