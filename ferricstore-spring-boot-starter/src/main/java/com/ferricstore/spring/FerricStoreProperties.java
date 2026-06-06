package com.ferricstore.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ferricstore")
public class FerricStoreProperties {
    /**
     * Redis-compatible FerricStore URL used by the SDK client.
     */
    private String url = "redis://127.0.0.1:6379/0";
    /**
     * Codec used for payloads, results, and named values.
     */
    private CodecMode codec = CodecMode.RAW;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public CodecMode getCodec() {
        return codec;
    }

    public void setCodec(CodecMode codec) {
        this.codec = codec;
    }

    public enum CodecMode {
        RAW,
        JSON,
        STRING
    }
}
