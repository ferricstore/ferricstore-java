package com.ferricstore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class CommandArgs {
    private CommandArgs() {
    }

    static List<Object> args(Object... values) {
        List<Object> args = new ArrayList<>(values.length);
        for (Object value : values) {
            args.add(value);
        }
        return args;
    }

    static void append(List<Object> args, String name, Object value) {
        if (value == null) {
            return;
        }
        if (value instanceof String text && text.isEmpty()) {
            return;
        }
        args.add(name);
        args.add(value);
    }

    static void appendBool(List<Object> args, String name, Boolean value) {
        if (value != null) {
            args.add(name);
            args.add(value ? "true" : "false");
        }
    }

    static void appendEncoded(List<Object> args, String name, Codec codec, Object value) {
        if (value != null) {
            args.add(name);
            args.add(codec.encode(value));
        }
    }

    static void appendNamedValues(List<Object> args, Codec codec, Map<String, ?> values, Map<String, String> valueRefs) {
        if (values != null) {
            values.forEach((name, value) -> {
                args.add("VALUE");
                args.add(name);
                args.add(codec.encode(value));
            });
        }
        if (valueRefs != null) {
            valueRefs.forEach((name, ref) -> {
                args.add("VALUE_REF");
                args.add(name);
                args.add(ref);
            });
        }
    }

    static void appendPayloadRead(List<Object> args, Boolean payload, Long maxBytes) {
        if (payload != null) {
            args.add("PAYLOAD");
            args.add(payload ? "true" : "false");
        }
        if (maxBytes != null) {
            args.add("PAYLOAD_MAX_BYTES");
            args.add(maxBytes);
        }
    }

    static boolean ok(Object value) {
        if (value == null) {
            return false;
        }
        return "OK".equalsIgnoreCase(Resp.string(value)) || Boolean.TRUE.equals(value);
    }
}
