package com.ferricstore;

public record SetOptions(
        Long exSeconds,
        Long pxMilliseconds,
        Long exatSeconds,
        Long pxatMillis,
        boolean nx,
        boolean xx,
        boolean get,
        boolean keepTtl) {
    public SetOptions {
        if (nx && xx) {
            throw new IllegalArgumentException("SET NX and XX options are mutually exclusive");
        }
        int expiryModes =
                present(exSeconds)
                        + present(pxMilliseconds)
                        + present(exatSeconds)
                        + present(pxatMillis);
        if (expiryModes > 1) {
            throw new IllegalArgumentException("SET accepts only one expiration option");
        }
        if (keepTtl && expiryModes != 0) {
            throw new IllegalArgumentException(
                    "SET KEEPTTL and expiration options are mutually exclusive");
        }
        requirePositive(exSeconds, "EX");
        requirePositive(pxMilliseconds, "PX");
        requirePositive(exatSeconds, "EXAT");
        requirePositive(pxatMillis, "PXAT");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Long exSeconds;
        private Long pxMilliseconds;
        private Long exatSeconds;
        private Long pxatMillis;
        private boolean nx;
        private boolean xx;
        private boolean get;
        private boolean keepTtl;

        public Builder exSeconds(long value) {
            exSeconds = value;
            return this;
        }

        public Builder pxMilliseconds(long value) {
            pxMilliseconds = value;
            return this;
        }

        public Builder exatSeconds(long value) {
            exatSeconds = value;
            return this;
        }

        public Builder pxatMillis(long value) {
            pxatMillis = value;
            return this;
        }

        public Builder nx(boolean value) {
            nx = value;
            return this;
        }

        public Builder xx(boolean value) {
            xx = value;
            return this;
        }

        public Builder get(boolean value) {
            get = value;
            return this;
        }

        public Builder keepTtl(boolean value) {
            keepTtl = value;
            return this;
        }

        public SetOptions build() {
            return new SetOptions(
                    exSeconds, pxMilliseconds, exatSeconds, pxatMillis, nx, xx, get, keepTtl);
        }
    }

    private static int present(Long value) {
        return value == null ? 0 : 1;
    }

    private static void requirePositive(Long value, String name) {
        if (value != null && value <= 0) {
            throw new IllegalArgumentException("SET " + name + " must be positive");
        }
    }
}
