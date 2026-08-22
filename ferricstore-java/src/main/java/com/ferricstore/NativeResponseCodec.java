package com.ferricstore;

final class NativeResponseCodec {
    record Response(int status, Object value) {}

    private NativeResponseCodec() {}

    static Response decode(byte[] body) {
        if (body.length < 3) {
            throw new NativeProtocolException(
                    "native response body must contain a status and one typed value");
        }
        int status = (Byte.toUnsignedInt(body[0]) << 8) | Byte.toUnsignedInt(body[1]);
        return new Response(status, NativeValueCodec.decode(body, 2));
    }

    static Object requireOk(Response response) {
        if (response.status() != NativeProtocol.STATUS_OK) {
            throw new NativeServerException(response.status(), response.value());
        }
        return response.value();
    }
}
