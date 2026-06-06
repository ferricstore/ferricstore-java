package com.ferricstore;

public record FetchOrComputeResult(
        String status, Object value, String computeToken, boolean hit, boolean shouldCompute) {}
