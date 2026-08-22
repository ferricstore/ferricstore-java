package com.ferricstore;

/** A finite Flow active-lifetime bound, or an explicit infinite lifetime. */
public sealed interface MaxActiveMs permits MaxActiveMs.Finite, MaxActiveMs.Infinity {
    long MAX_FINITE_MILLISECONDS = 31_536_000_000L;

    static MaxActiveMs of(long milliseconds) {
        return new Finite(milliseconds);
    }

    static MaxActiveMs infinity() {
        return Infinity.INSTANCE;
    }

    record Finite(long milliseconds) implements MaxActiveMs {
        public Finite {
            if (milliseconds <= 0 || milliseconds > MAX_FINITE_MILLISECONDS) {
                throw new IllegalArgumentException(
                        "flow max_active_ms must be between 1 and "
                                + MAX_FINITE_MILLISECONDS
                                + " or infinity");
            }
        }
    }

    final class Infinity implements MaxActiveMs {
        private static final Infinity INSTANCE = new Infinity();

        private Infinity() {}

        @Override
        public String toString() {
            return "infinity";
        }
    }
}
