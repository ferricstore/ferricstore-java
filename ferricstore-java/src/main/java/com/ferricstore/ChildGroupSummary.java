package com.ferricstore;

/** A child group's children counted by outcome. */
public record ChildGroupSummary(long total, long completed, long failed, long cancelled) {}
