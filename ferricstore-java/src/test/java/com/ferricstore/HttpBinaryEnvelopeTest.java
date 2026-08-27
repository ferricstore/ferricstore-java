package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class HttpBinaryEnvelopeTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void streamingWriterMatchesTheEstablishedBinarySafeEnvelope() throws Exception {
        Map<Object, Object> nested = new LinkedHashMap<>();
        nested.put(new byte[] {0, 1}, List.of("text", true, 42L));
        nested.put("buffer", ByteBuffer.wrap(new byte[] {(byte) 0xff, 2}));
        List<Object> value = new ArrayList<>();
        value.add(nested);
        value.add(null);
        value.add(new Object[] {1.25d, "done"});

        JsonNode established =
                JSON.readTree(JSON.writeValueAsBytes(HttpBinaryEnvelope.encode(value)));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JsonGenerator output = JSON.getFactory().createGenerator(bytes)) {
            HttpBinaryEnvelope.writeJson(output, value);
        }

        assertEquals(established, JSON.readTree(bytes.toByteArray()));
    }
}
