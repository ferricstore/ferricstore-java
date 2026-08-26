package com.ferricstore;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Streaming decoder for the binary-safe HTTP command response envelope. */
final class HttpResponseDecoder {
    private static final JsonFactory JSON = new JsonFactory();

    private HttpResponseDecoder() {}

    static Map<String, Object> decode(byte[] body) throws IOException {
        try (JsonParser input = JSON.createParser(body)) {
            if (input.nextToken() != JsonToken.START_OBJECT) {
                throw new IOException("HTTP response root must be an object");
            }
            return readResponse(input);
        }
    }

    private static Map<String, Object> readResponse(JsonParser input) throws IOException {
        Map<String, Object> response = new LinkedHashMap<>(4);
        while (input.nextToken() != JsonToken.END_OBJECT) {
            String field = requireFieldName(input);
            JsonToken value = requireValue(input);
            response.put(
                    field,
                    "results".equals(field)
                            ? readResults(input, value)
                            : readPlainValue(input, value));
        }
        return response;
    }

    private static Object readResults(JsonParser input, JsonToken token) throws IOException {
        if (token != JsonToken.START_ARRAY) {
            return readPlainValue(input, token);
        }
        List<Object> results = new ArrayList<>(8);
        JsonToken item;
        while ((item = input.nextToken()) != JsonToken.END_ARRAY) {
            results.add(
                    item == JsonToken.START_OBJECT
                            ? readCommandResult(input)
                            : readPlainValue(input, item));
        }
        return results;
    }

    private static Map<String, Object> readCommandResult(JsonParser input) throws IOException {
        Map<String, Object> result = new LinkedHashMap<>(4);
        while (input.nextToken() != JsonToken.END_OBJECT) {
            String field = requireFieldName(input);
            JsonToken value = requireValue(input);
            result.put(
                    field,
                    "value".equals(field)
                            ? readEnvelopeValue(input, value)
                            : readPlainValue(input, value));
        }
        return result;
    }

    private static Object readPlainValue(JsonParser input, JsonToken token) throws IOException {
        return switch (token) {
            case START_OBJECT -> readPlainObject(input);
            case START_ARRAY -> readPlainArray(input);
            case VALUE_STRING -> input.getText();
            case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> input.getNumberValue();
            case VALUE_TRUE -> Boolean.TRUE;
            case VALUE_FALSE -> Boolean.FALSE;
            case VALUE_NULL -> null;
            default -> throw new IOException("unexpected JSON response token " + token);
        };
    }

    private static Map<String, Object> readPlainObject(JsonParser input) throws IOException {
        Map<String, Object> result = new LinkedHashMap<>(4);
        while (input.nextToken() != JsonToken.END_OBJECT) {
            String field = requireFieldName(input);
            result.put(field, readPlainValue(input, requireValue(input)));
        }
        return result;
    }

    private static List<Object> readPlainArray(JsonParser input) throws IOException {
        List<Object> result = new ArrayList<>(8);
        JsonToken item;
        while ((item = input.nextToken()) != JsonToken.END_ARRAY) {
            result.add(readPlainValue(input, item));
        }
        return result;
    }

    private static Object readEnvelopeValue(JsonParser input, JsonToken token) throws IOException {
        if (token == JsonToken.START_OBJECT) {
            return readEnvelopeObject(input);
        }
        if (token == JsonToken.START_ARRAY) {
            List<Object> result = new ArrayList<>(8);
            JsonToken item;
            while ((item = input.nextToken()) != JsonToken.END_ARRAY) {
                result.add(readEnvelopeValue(input, item));
            }
            return result;
        }
        return readPlainValue(input, token);
    }

    private static Object readEnvelopeObject(JsonParser input) throws IOException {
        JsonToken token = input.nextToken();
        if (token == JsonToken.END_OBJECT) {
            return new LinkedHashMap<>();
        }
        String firstField = requireFieldName(input);
        Object firstValue = readEnvelopeValue(input, requireValue(input));
        token = input.nextToken();
        if (token == JsonToken.END_OBJECT) {
            return decodeSingleFieldEnvelope(firstField, firstValue);
        }

        Map<String, Object> result = new LinkedHashMap<>(4);
        result.put(firstField, firstValue);
        while (token != JsonToken.END_OBJECT) {
            String field = requireFieldName(input);
            result.put(field, readEnvelopeValue(input, requireValue(input)));
            token = input.nextToken();
        }
        return result;
    }

    private static Object decodeSingleFieldEnvelope(String field, Object value) {
        if (HttpBinaryEnvelope.BYTES_TAG.equals(field)) {
            if (!(value instanceof String text)) {
                throw malformed("binary marker payload must be text");
            }
            try {
                return Base64.getDecoder().decode(text);
            } catch (IllegalArgumentException error) {
                throw malformed("binary marker payload is not valid base64", error);
            }
        }
        if (!HttpBinaryEnvelope.MAP_TAG.equals(field)) {
            Map<String, Object> result = new LinkedHashMap<>(2);
            result.put(field, value);
            return result;
        }
        if (!(value instanceof List<?> pairs)) {
            throw malformed("map marker payload must be a list");
        }
        Map<Object, Object> decoded = new LinkedHashMap<>(mapCapacity(pairs.size()));
        for (Object entry : pairs) {
            if (!(entry instanceof List<?> pair) || pair.size() != 2) {
                throw malformed("map marker entries must be key/value pairs");
            }
            decoded.put(pair.get(0), pair.get(1));
        }
        return decoded;
    }

    private static int mapCapacity(int entries) {
        if (entries < 3) {
            return 4;
        }
        long capacity = entries * 4L / 3L + 1L;
        return (int) Math.min(1 << 30, capacity);
    }

    private static String requireFieldName(JsonParser input) throws IOException {
        if (input.currentToken() != JsonToken.FIELD_NAME) {
            throw new IOException("expected a JSON object field");
        }
        return input.currentName();
    }

    private static JsonToken requireValue(JsonParser input) throws IOException {
        JsonToken token = input.nextToken();
        if (token == null) {
            throw new IOException("unexpected end of JSON response");
        }
        return token;
    }

    static final class MalformedEnvelopeException extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;

        private MalformedEnvelopeException(String message) {
            super(message);
        }

        private MalformedEnvelopeException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static MalformedEnvelopeException malformed(String message) {
        return new MalformedEnvelopeException(message);
    }

    private static MalformedEnvelopeException malformed(String message, Throwable cause) {
        return new MalformedEnvelopeException(message, cause);
    }
}
