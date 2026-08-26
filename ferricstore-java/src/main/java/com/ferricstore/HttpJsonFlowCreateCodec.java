package com.ferricstore;

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;

/** Streaming JSON writer for a homogeneous FLOW.CREATE pipeline. */
final class HttpJsonFlowCreateCodec {
    private HttpJsonFlowCreateCodec() {}

    static void writePayload(JsonGenerator output, FlowCreatePipeline.Batch batch)
            throws IOException {
        output.writeStartObject();
        output.writeArrayFieldStart(HttpBinaryEnvelope.MAP_TAG);

        writePairName(output, "items");
        output.writeStartArray();
        for (FlowCreatePipeline.CreateItem item : batch.items()) {
            output.writeStartObject();
            output.writeArrayFieldStart(HttpBinaryEnvelope.MAP_TAG);
            writePairName(output, "id");
            HttpBinaryEnvelope.writeJson(output, item.id());
            output.writeEndArray();
            if (item.payloadPresent()) {
                writePairName(output, "payload");
                HttpBinaryEnvelope.writeJson(output, item.payload());
                output.writeEndArray();
            }
            output.writeEndArray();
            output.writeEndObject();
        }
        output.writeEndArray();
        output.writeEndArray();

        FlowCreatePipeline.Metadata metadata = batch.metadata();
        writePairName(output, "type");
        HttpBinaryEnvelope.writeJson(output, metadata.type());
        output.writeEndArray();
        writePairName(output, "state");
        HttpBinaryEnvelope.writeJson(output, metadata.state());
        output.writeEndArray();
        writePairName(output, "now_ms");
        output.writeNumber(metadata.nowMs());
        output.writeEndArray();
        writePairName(output, "run_at_ms");
        output.writeNumber(metadata.runAtMs());
        output.writeEndArray();
        writePairName(output, "priority");
        output.writeNumber(0L);
        output.writeEndArray();
        writePairName(output, "independent");
        output.writeBoolean(true);
        output.writeEndArray();
        writePairName(output, "return");
        output.writeString("ok_on_success");
        output.writeEndArray();
        output.writeEndArray();
        output.writeEndObject();
    }

    private static void writePairName(JsonGenerator output, String name) throws IOException {
        output.writeStartArray();
        output.writeString(name);
    }
}
