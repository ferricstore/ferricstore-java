package com.ferricstore;

/** Optional width/depth dimensions for {@code TOPK.RESERVE}; decay is not part of 0.8. */
public record TopKReserveOptions(Long width, Long depth) {
    public TopKReserveOptions {
        if (width == null && depth != null || width != null && depth == null) {
            throw new IllegalArgumentException(
                    "TOPK.RESERVE width and depth must be provided together");
        }
        if (width != null && width <= 0 || depth != null && depth <= 0) {
            throw new IllegalArgumentException("TOPK.RESERVE width and depth must be positive");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Long width;
        private Long depth;

        public Builder width(long value) {
            width = value;
            return this;
        }

        public Builder depth(long value) {
            depth = value;
            return this;
        }

        public TopKReserveOptions build() {
            return new TopKReserveOptions(width, depth);
        }
    }
}
