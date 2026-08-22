package com.ferricstore;

public record FetchOrComputeResult(
        String status,
        Object value,
        String computeHint,
        Object ownershipToken,
        boolean hit,
        boolean shouldCompute) {}
