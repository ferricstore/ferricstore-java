package com.ferricstore;

/** Conditional and changed-count options for {@code GEOADD}. */
public record GeoAddOptions(boolean nx, boolean xx, boolean ch) {
    public GeoAddOptions {
        if (nx && xx) {
            throw new IllegalArgumentException("GEOADD NX and XX options are mutually exclusive");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private boolean nx;
        private boolean xx;
        private boolean ch;

        public Builder nx(boolean value) {
            nx = value;
            return this;
        }

        public Builder xx(boolean value) {
            xx = value;
            return this;
        }

        public Builder ch(boolean value) {
            ch = value;
            return this;
        }

        public GeoAddOptions build() {
            return new GeoAddOptions(nx, xx, ch);
        }
    }
}
