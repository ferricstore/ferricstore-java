package com.ferricstore;

import java.nio.charset.StandardCharsets;
import java.util.List;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.commands.ProtocolCommand;

public final class JedisRedisExecutor implements RedisExecutor, AutoCloseable {
    private final UnifiedJedis jedis;
    private final boolean closeClient;

    public JedisRedisExecutor(UnifiedJedis jedis, boolean closeClient) {
        this.jedis = jedis;
        this.closeClient = closeClient;
    }

    public static JedisRedisExecutor connect(String uri) {
        return new JedisRedisExecutor(new JedisPooled(uri), true);
    }

    @Override
    public Object execute(List<Object> args) {
        if (args.isEmpty()) {
            throw new IllegalArgumentException("empty Redis command");
        }
        ProtocolCommand command = new RawCommand(bytes(args.getFirst()));
        byte[][] commandArgs = new byte[args.size() - 1][];
        for (int i = 1; i < args.size(); i++) {
            commandArgs[i - 1] = bytes(args.get(i));
        }
        return jedis.sendCommand(command, commandArgs);
    }

    @Override
    public void close() {
        if (!closeClient) {
            return;
        }
        jedis.close();
    }

    private static byte[] bytes(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }

    private record RawCommand(byte[] raw) implements ProtocolCommand {
        @Override
        public byte[] getRaw() {
            return raw;
        }
    }
}
