package com.ferricstore;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

record NativeEndpoint(String host, int port, boolean tls, String username, String password) {
    private static final int DEFAULT_PORT = 6388;

    NativeEndpoint {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("FerricStore URI requires a host");
        }
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("FerricStore URI port must be between 1 and 65535");
        }
        username = username == null || username.isEmpty() ? "default" : username;
    }

    static NativeEndpoint parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("FerricStore URI must not be blank");
        }
        final URI uri;
        try {
            uri = new URI(value);
        } catch (URISyntaxException error) {
            throw new IllegalArgumentException("invalid FerricStore URI", error);
        }
        String scheme =
                uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(java.util.Locale.ROOT);
        if (!"ferric".equals(scheme) && !"ferrics".equals(scheme)) {
            throw new IllegalArgumentException(
                    "FerricStore native URLs must use ferric:// or ferrics://");
        }
        if (uri.getHost() == null) {
            throw new IllegalArgumentException("FerricStore URI requires a valid host");
        }
        String path = uri.getRawPath();
        if ((path != null && !path.isEmpty() && !"/".equals(path))
                || uri.getRawQuery() != null
                || uri.getRawFragment() != null) {
            throw new IllegalArgumentException(
                    "FerricStore native URI must not contain a path, query, or fragment");
        }

        String username = null;
        String password = null;
        String userInfo = uri.getRawUserInfo();
        if (userInfo != null) {
            int separator = userInfo.indexOf(':');
            if (separator < 0) {
                username = percentDecode(userInfo);
            } else {
                username = percentDecode(userInfo.substring(0, separator));
                password = percentDecode(userInfo.substring(separator + 1));
            }
        }
        return new NativeEndpoint(
                uri.getHost(),
                uri.getPort() < 0 ? DEFAULT_PORT : uri.getPort(),
                "ferrics".equals(scheme),
                username,
                password);
    }

    private static String percentDecode(String value) {
        try {
            return URLDecoder.decode(value.replace("+", "%2B"), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException error) {
            throw new IllegalArgumentException(
                    "invalid percent encoding in FerricStore URI", error);
        }
    }
}
