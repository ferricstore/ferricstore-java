package com.ferricstore;

final class NativeProtocol {
    static final byte[] MAGIC = {'F', 'S', 'N', 'P'};
    static final int REQUEST_VERSION = 0x01;
    static final int RESPONSE_VERSION = 0x81;
    static final int HEADER_BYTES = 24;

    static final int FLAG_CUSTOM_PAYLOAD = 0x02;
    static final int FLAG_MORE_CHUNKS = 0x20;

    static final int OP_HELLO = 0x0001;
    static final int OP_AUTH = 0x0002;
    static final int OP_QUIT = 0x0009;
    static final int OP_COMMAND_EXEC = 0x0100;

    static final int STATUS_OK = 0;
    static final int STATUS_ERROR = 1;
    static final int STATUS_AUTH = 2;
    static final int STATUS_NOPERM = 3;
    static final int STATUS_BUSY = 4;
    static final int STATUS_REROUTE = 5;
    static final int STATUS_BAD_REQUEST = 6;

    static final int DEFAULT_MAX_RESPONSE_BYTES = 64 * 1024 * 1024;
    static final int DEFAULT_MAX_RESPONSE_CHUNKS = 1_024;
    static final int UNAUTHENTICATED_MAX_FRAME_BYTES = 64 * 1024;

    private NativeProtocol() {}
}
