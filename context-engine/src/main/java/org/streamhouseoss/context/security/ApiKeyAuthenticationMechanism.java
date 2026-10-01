package org.streamhouseoss.context.security;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkus.oidc.AccessTokenCredential;
import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.AuthenticationRequest;
import io.quarkus.security.identity.request.TokenAuthenticationRequest;
import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.smallrye.mutiny.Uni;
import io.quarkus.vertx.http.runtime.security.HttpSecurityUtils;
import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * API-key authentication: {@code Authorization: Basic base64(key:secret)}, where the key and secret
 * are an OIDC client's id and secret. The pair is exchanged for an access token with the client
 * credentials grant (cached until shortly before it expires) and then authenticated exactly like a
 * bearer token, so roles and grants apply unchanged. Requests without Basic credentials fall
 * through to the bearer-token mechanism.
 */
@ApplicationScoped
@Priority(2000)
public class ApiKeyAuthenticationMechanism implements HttpAuthenticationMechanism {

    private record CachedToken(String accessToken, Instant expiresAt) {
    }

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final Map<String, CachedToken> tokens = new ConcurrentHashMap<>();
    private final ObjectMapper mapper;
    private final String tokenUrl;

    public ApiKeyAuthenticationMechanism(ObjectMapper mapper,
            @ConfigProperty(name = "quarkus.oidc.auth-server-url") String authServerUrl,
            @ConfigProperty(name = "streamhouse.context.api-key.token-url") Optional<String> tokenUrl) {
        this.mapper = mapper;
        // Default: the Keycloak token endpoint of the configured realm.
        this.tokenUrl = tokenUrl.orElse(authServerUrl.replaceAll("/+$", "") + "/protocol/openid-connect/token");
    }

    @Override
    public Uni<SecurityIdentity> authenticate(RoutingContext context, IdentityProviderManager identityProviderManager) {
        String header = context.request().getHeader("Authorization");
        if (header == null || !header.regionMatches(true, 0, "Basic ", 0, 6)) {
            return Uni.createFrom().nullItem();
        }
        String decoded;
        try {
            decoded = new String(Base64.getDecoder().decode(header.substring(6).trim()), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return Uni.createFrom().failure(new AuthenticationFailedException("malformed Basic credentials"));
        }
        int colon = decoded.indexOf(':');
        if (colon <= 0) {
            return Uni.createFrom().failure(new AuthenticationFailedException("Basic credentials must be key:secret"));
        }
        String key = decoded.substring(0, colon);
        String secret = decoded.substring(colon + 1);
        // Resume on the request's Vert.x context: the OIDC provider and the HTTP layer expect it.
        io.vertx.core.Context vertxContext = Vertx.currentContext();
        return Uni.createFrom().completionStage(accessToken(key, secret))
                .emitOn(task -> vertxContext.runOnContext(ignored -> task.run()))
                .onItem().transformToUni(token -> {
                    TokenAuthenticationRequest request = new TokenAuthenticationRequest(new AccessTokenCredential(token));
                    HttpSecurityUtils.setRoutingContextAttribute(request, context);
                    return identityProviderManager.authenticate(request);
                });
    }

    private CompletableFuture<String> accessToken(String key, String secret) {
        String cacheKey = sha256(key + ":" + secret);
        CachedToken cached = tokens.get(cacheKey);
        if (cached != null && Instant.now().isBefore(cached.expiresAt())) {
            return CompletableFuture.completedFuture(cached.accessToken());
        }
        String form = "grant_type=client_credentials&client_id=" + URLEncoder.encode(key, StandardCharsets.UTF_8)
                + "&client_secret=" + URLEncoder.encode(secret, StandardCharsets.UTF_8);
        HttpRequest request = HttpRequest.newBuilder(URI.create(tokenUrl))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form))
                .build();
        return http.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply(response -> {
            if (response.statusCode() != 200) {
                throw new AuthenticationFailedException("invalid API key or secret");
            }
            try {
                JsonNode body = mapper.readTree(response.body());
                String token = body.path("access_token").asText();
                long ttl = Math.max(0, body.path("expires_in").asLong(60) - 30);
                tokens.put(cacheKey, new CachedToken(token, Instant.now().plusSeconds(ttl)));
                return token;
            } catch (Exception e) {
                throw new AuthenticationFailedException("cannot read the token response", e);
            }
        });
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
