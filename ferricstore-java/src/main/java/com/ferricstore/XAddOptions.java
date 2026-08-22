package com.ferricstore;

/** Options for {@code XADD}, including exact or approximate trimming. */
public record XAddOptions(
        String id, Long maxlen, String minid, boolean noMkStream, boolean approximate) {
    public XAddOptions {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("XADD id must not be blank");
        }
        if (maxlen != null && maxlen < 0) {
            throw new IllegalArgumentException("XADD MAXLEN must be non-negative");
        }
        if (maxlen != null && minid != null) {
            throw new IllegalArgumentException("XADD accepts only one of MAXLEN or MINID");
        }
        if (minid != null && minid.isBlank()) {
            throw new IllegalArgumentException("XADD MINID must not be blank");
        }
        if (approximate && maxlen == null && minid == null) {
            throw new IllegalArgumentException(
                    "XADD approximate trimming requires MAXLEN or MINID");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String id = "*";
        private Long maxlen;
        private String minid;
        private boolean noMkStream;
        private boolean approximate;

        public Builder id(String value) {
            id = value;
            return this;
        }

        public Builder maxlen(long value) {
            maxlen = value;
            return this;
        }

        public Builder minid(String value) {
            minid = value;
            return this;
        }

        public Builder noMkStream(boolean value) {
            noMkStream = value;
            return this;
        }

        public Builder approximate(boolean value) {
            approximate = value;
            return this;
        }

        public XAddOptions build() {
            return new XAddOptions(id, maxlen, minid, noMkStream, approximate);
        }
    }
}
