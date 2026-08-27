package com.ferricstore;

/** Conditional and response options for {@code ZADD}. */
public record ZAddOptions(
        boolean nx, boolean xx, boolean gt, boolean lt, boolean ch, boolean incr) {
    public ZAddOptions {
        if (nx && xx) {
            throw new IllegalArgumentException("ZADD NX and XX options are mutually exclusive");
        }
        if (gt && lt) {
            throw new IllegalArgumentException("ZADD GT and LT options are mutually exclusive");
        }
        if (nx && (gt || lt)) {
            throw new IllegalArgumentException("ZADD NX cannot be combined with GT or LT");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private boolean nx;
        private boolean xx;
        private boolean gt;
        private boolean lt;
        private boolean ch;
        private boolean incr;

        public Builder nx(boolean value) {
            nx = value;
            return this;
        }

        public Builder xx(boolean value) {
            xx = value;
            return this;
        }

        public Builder gt(boolean value) {
            gt = value;
            return this;
        }

        public Builder lt(boolean value) {
            lt = value;
            return this;
        }

        public Builder ch(boolean value) {
            ch = value;
            return this;
        }

        public Builder incr(boolean value) {
            incr = value;
            return this;
        }

        public ZAddOptions build() {
            return new ZAddOptions(nx, xx, gt, lt, ch, incr);
        }
    }
}
