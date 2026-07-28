-- release_lock.lua
-- Compara token e deleta: evita liberar lock de outro processo (Seção 6.3.2).
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
