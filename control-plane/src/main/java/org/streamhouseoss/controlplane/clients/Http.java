package org.streamhouseoss.controlplane.clients;

import org.jboss.resteasy.reactive.RestResponse;

/** Status handling for REST client responses (the default exception mapper is disabled). */
final class Http {

    private Http() {
    }

    static <T> RestResponse<T> ok(RestResponse<T> response, String what) {
        if (response.getStatus() >= 300) {
            String body = body(response);
            throw new ComponentException(what + " failed with HTTP " + response.getStatus()
                    + (body.isBlank() ? "" : ": " + abbreviate(body)));
        }
        return response;
    }

    private static String body(RestResponse<?> response) {
        try {
            return response.hasEntity() ? String.valueOf(response.getEntity()) : "";
        } catch (RuntimeException unreadable) {
            return "";
        }
    }

    static boolean notFound(RestResponse<?> response) {
        return response.getStatus() == 404;
    }

    static String abbreviate(String s) {
        return s.length() > 600 ? s.substring(0, 600) + "…" : s;
    }
}
