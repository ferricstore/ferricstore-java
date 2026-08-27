package com.ferricstore;

import java.util.List;
import java.util.Map;

public final class StreamStore {
    private final FerricStoreClient client;

    StreamStore(FerricStoreClient client) {
        this.client = client;
    }

    public Object xadd(String key, String id, Map<String, ?> fields) {
        return xadd(key, fields, XAddOptions.builder().id(id).build());
    }

    public Object xadd(String key, Map<String, ?> fields, XAddOptions options) {
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("XADD requires at least one field/value pair");
        }
        java.util.Objects.requireNonNull(options, "XADD options");
        List<Object> args = CommandArgs.args("XADD", key);
        if (options.noMkStream()) {
            args.add("NOMKSTREAM");
        }
        if (options.maxlen() != null) {
            args.add("MAXLEN");
            if (options.approximate()) {
                args.add("~");
            }
            args.add(options.maxlen());
        } else if (options.minid() != null) {
            args.add("MINID");
            if (options.approximate()) {
                args.add("~");
            }
            args.add(options.minid());
        }
        args.add(options.id());
        fields.forEach(
                (field, value) -> {
                    args.add(field);
                    args.add(client.codec().encode(value));
                });
        return client.command(args);
    }

    public long xlen(String key) {
        return Resp.number(client.command("XLEN", key));
    }

    public List<Object> xrange(String key, String start, String end) {
        return xrange(key, start, end, new Object[0]);
    }

    public List<Object> xrange(String key, String start, String end, Object... options) {
        List<Object> args = CommandArgs.args("XRANGE", key, start, end);
        args.addAll(List.of(options));
        return Resp.list(client.command(args));
    }

    public List<Object> xrevrange(String key, String end, String start, Object... options) {
        List<Object> args = CommandArgs.args("XREVRANGE", key, end, start);
        args.addAll(List.of(options));
        return Resp.list(client.command(args));
    }

    public Object xread(Map<String, String> streams, Integer count, Long blockMs) {
        List<Object> args = CommandArgs.args("XREAD");
        CommandArgs.append(args, "COUNT", count);
        CommandArgs.append(args, "BLOCK", blockMs);
        appendStreams(args, streams);
        return client.command(args);
    }

    public Object xtrim(String key, Object... options) {
        List<Object> args = CommandArgs.args("XTRIM", key);
        args.addAll(List.of(options));
        return client.command(args);
    }

    public long xdel(String key, String... ids) {
        List<Object> args = CommandArgs.args("XDEL", key);
        args.addAll(List.of(ids));
        return Resp.number(client.command(args));
    }

    public Object xinfo(String subcommand, String key, Object... options) {
        List<Object> args = CommandArgs.args("XINFO", subcommand, key);
        args.addAll(List.of(options));
        return client.command(args);
    }

    public Object xgroup(String subcommand, String key, String group, Object... options) {
        List<Object> args = CommandArgs.args("XGROUP", subcommand, key, group);
        args.addAll(List.of(options));
        return client.command(args);
    }

    public Object xreadgroup(
            String group,
            String consumer,
            Map<String, String> streams,
            Integer count,
            Long blockMs,
            boolean noAck) {
        List<Object> args = CommandArgs.args("XREADGROUP", "GROUP", group, consumer);
        CommandArgs.append(args, "COUNT", count);
        CommandArgs.append(args, "BLOCK", blockMs);
        if (noAck) {
            args.add("NOACK");
        }
        appendStreams(args, streams);
        return client.command(args);
    }

    public long xack(String key, String group, String... ids) {
        List<Object> args = CommandArgs.args("XACK", key, group);
        args.addAll(List.of(ids));
        return Resp.number(client.command(args));
    }

    private static void appendStreams(List<Object> args, Map<String, String> streams) {
        if (streams == null || streams.isEmpty()) {
            throw new IllegalArgumentException("stream read requires at least one stream");
        }
        HashSlot.requireSame("stream read", List.copyOf(streams.keySet()));
        args.add("STREAMS");
        args.addAll(streams.keySet());
        args.addAll(streams.values());
    }
}
