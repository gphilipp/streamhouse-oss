package org.streamhouseoss.context.security;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.streamhouseoss.context.ContextConfig;

import io.quarkus.oidc.AccessTokenCredential;
import io.quarkus.oidc.client.OidcClient;
import io.quarkus.oidc.client.OidcClientConfigBuilder;
import io.quarkus.oidc.client.OidcClients;
import io.quarkus.oidc.client.Tokens;
import io.quarkus.oidc.client.runtime.OidcClientConfig;
import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.AuthenticationRequest;
import io.quarkus.security.identity.request.TokenAuthenticationRequest;
import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.quarkus.vertx.http.runtime.security.HttpSecurityUtils;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.RoutingContext;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * API-key authentication: {@code Authorization: Basic base64(key:secret)}, where the key and secret
 * are an OIDC client's id and secret. The pair is exchanged for an access token with the client
 * credentials grant (one OIDC client per key, tokens reused until they near expiry) and then
 * authenticated exactly like a bearer token, so roles and grants apply unchanged. Requests without
 * Basic credentials fall through to the bearer-token mechanism.
 */
@ApplicationScoped
@Priority(2000)
public class ApiKeyAuthenticationMechanism implements HttpAuthenticationMechanism {

    /** An OIDC client for one key:secret pair and the last tokens it obtained. */
    private static final class ApiKey {
        final Uni<OidcClient> client;
        volatile Tokens tokens;

        ApiKey(Uni<OidcClient> client) {
            this.client = client.memoize().indefinitely();
        }
    }

    private final OidcClients oidcClients;
    private final String authServerUrl;
    private final Optional<String> tokenUrl;
    private final Map<String, ApiKey> keys = new ConcurrentHashMap<>();

    public ApiKeyAuthenticationMechanism(OidcClients oidcClients, ContextConfig config,
            @ConfigProperty(name = "quarkus.oidc.auth-server-url") String authServerUrl) {
        this.oidcClients = oidcClients;
        this.authServerUrl = authServerUrl;
        this.tokenUrl = config.apiKeyTokenUrl();
    }

    @Override
    public Uni<SecurityIdentity> authenticate(RoutingContext context, IdentityProviderManager identityProviderManager) {
        Optional<String[]> credentials;
        try {
            credentials = basicCredentials(context.request().getHeader("Authorization"));
        } catch (AuthenticationFailedException e) {
            return Uni.createFrom().failure(e);
        }
        if (credentials.isEmpty()) {
            return Uni.createFrom().nullItem();
        }
        String key = credentials.get()[0];
        String secret = credentials.get()[1];
        return accessToken(key, secret).onItem().transformToUni(token -> {
            TokenAuthenticationRequest request = new TokenAuthenticationRequest(new AccessTokenCredential(token));
            HttpSecurityUtils.setRoutingContextAttribute(request, context);
            return identityProviderManager.authenticate(request);
        });
    }

    /** The key and secret of a {@code Basic} authorization header, or empty for any other scheme. */
    static Optional<String[]> basicCredentials(String header) {
        if (header == null || !header.regionMatches(true, 0, "Basic ", 0, 6)) {
            return Optional.empty();
        }
        String decoded;
        try {
            decoded = new String(Base64.getDecoder().decode(header.substring(6).trim()), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new AuthenticationFailedException("malformed Basic credentials");
        }
        int colon = decoded.indexOf(':');
        if (colon <= 0) {
            throw new AuthenticationFailedException("Basic credentials must be key:secret");
        }
        return Optional.of(new String[] { decoded.substring(0, colon), decoded.substring(colon + 1) });
    }

    private Uni<String> accessToken(String key, String secret) {
        String cacheKey = sha256(key + ":" + secret);
        ApiKey apiKey = keys.computeIfAbsent(cacheKey, k -> new ApiKey(oidcClients.newClient(clientConfig(key, secret))));
        Tokens current = apiKey.tokens;
        if (current != null && !current.isAccessTokenExpired() && !current.isAccessTokenWithinRefreshInterval()) {
            return Uni.createFrom().item(current.getAccessToken());
        }
        return apiKey.client
                .onItem().transformToUni(client -> client.getTokens())
                .onItem().transform(tokens -> {
                    apiKey.tokens = tokens;
                    return tokens.getAccessToken();
                })
                .onFailure().transform(e -> {
                    keys.remove(cacheKey); // don't keep clients for wrong secrets
                    return new AuthenticationFailedException("invalid API key or secret", e);
                });
    }

    private OidcClientConfig clientConfig(String key, String secret) {
        OidcClientConfigBuilder builder = OidcClientConfig.builder()
                .id("api-key-" + key)
                .authServerUrl(authServerUrl)
                .clientId(key)
                .credentials(secret)
                .grant(OidcClientConfig.Grant.Type.CLIENT)
                .refreshTokenTimeSkew(Duration.ofSeconds(30));
        tokenUrl.ifPresent(url -> builder.discoveryEnabled(false).tokenPath(url));
        return builder.build();
    }

    @Override
    public Uni<ChallengeData> getChallenge(RoutingContext context) {
        return Uni.createFrom().item(new ChallengeData(401, "WWW-Authenticate", "Basic realm=\"streamhouse\""));
    }

    @Override
    public Set<Class<? extends AuthenticationRequest>> getCredentialTypes() {
        return Set.of(TokenAuthenticationRequest.class);
    }

    private static String sha256(String value) {
        try {
            return Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
