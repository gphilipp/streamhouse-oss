package org.streamhouseoss.context.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.junit.jupiter.api.Test;

import io.quarkus.security.AuthenticationFailedException;

/** Header parsing only; the client-credentials exchange is exercised end to end by e2e/e2e.py. */
class ApiKeyAuthenticationMechanismTest {

    private static String basic(String credentials) {
        return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void parsesKeyAndSecret() {
        assertThat(ApiKeyAuthenticationMechanism.basicCredentials(basic("support-agent:s3:cr3t")))
                .hasValueSatisfying(c -> assertThat(c).containsExactly("support-agent", "s3:cr3t"));
        assertThat(ApiKeyAuthenticationMechanism.basicCredentials("basic " + basic("k:s").substring(6))).isPresent();
    }

    @Test
    void otherSchemesFallThrough() {
        assertThat(ApiKeyAuthenticationMechanism.basicCredentials(null)).isEmpty();
        assertThat(ApiKeyAuthenticationMechanism.basicCredentials("Bearer eyJhbGciOi")).isEmpty();
    }

    @Test
    void rejectsMalformedCredentials() {
        assertThatThrownBy(() -> ApiKeyAuthenticationMechanism.basicCredentials("Basic !!!"))
                .isInstanceOf(AuthenticationFailedException.class);
        assertThatThrownBy(() -> ApiKeyAuthenticationMechanism.basicCredentials(basic("no-colon")))
                .isInstanceOf(AuthenticationFailedException.class);
        assertThatThrownBy(() -> ApiKeyAuthenticationMechanism.basicCredentials(basic(":secret")))
                .isInstanceOf(AuthenticationFailedException.class);
    }
}
