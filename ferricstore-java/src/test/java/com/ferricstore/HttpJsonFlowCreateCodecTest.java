package com.ferricstore;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import org.junit.jupiter.api.Test;

final class HttpJsonFlowCreateCodecTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void writesCreateManyWithoutChangingItsBinaryEnvelope() throws Exception {
        FlowCreateCodecTestSupport.Fixture fixture = FlowCreateCodecTestSupport.fixture();

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JsonGenerator output = JSON.getFactory().createGenerator(bytes)) {
            HttpJsonFlowCreateCodec.writePayload(output, fixture.batch());
        }
        Object encoded = JSON.readValue(bytes.toByteArray(), Object.class);
        java.util.Map<?, ?> payload =
                assertInstanceOf(java.util.Map.class, HttpBinaryEnvelope.decode(encoded));
        FlowCreateCodecTestSupport.assertPayload(payload, fixture);
    }
}
