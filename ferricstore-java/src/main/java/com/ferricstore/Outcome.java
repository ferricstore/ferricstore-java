package com.ferricstore;

public sealed interface Outcome
        permits TransitionOutcome, CompleteOutcome, RetryOutcome, FailOutcome {}
