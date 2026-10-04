package org.streamhouseoss.cli;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import io.quarkus.oidc.client.OidcClient;
import io.quarkus.oidc.client.OidcClientException;
import io.quarkus.oidc.client.Tokens;
import io.quarkus.oidc.client.runtime.OidcClientConfig;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "login", description = "Log in with your browser (device flow), or with --username/--password for scripts.")
class LoginCommand extends BaseCommand {

    @Option(names = { "-u", "--username" }, description = "Log in with a password instead of the browser")
    String username;

    @Option(names = { "-p", "--password" }, description = "Password (env SHCTL_PASSWORD)", defaultValue = "${env:SHCTL_PASSWORD}")
    String password;

    @Override
    int run() {
        Session session = session();
        Tokens tokens;
        if (username != null) {
            if (password == null) {
                throw new CliException("--password (or SHCTL_PASSWORD) is required with --username");
            }
            OidcClient client = session.client(OidcClientConfig.Grant.Type.PASSWORD,
                    Map.of("username", username, "password", password));
            try {
                tokens = session.await(client.getTokens());
            } catch (OidcClientException e) {
                throw new CliException("login failed: " + e.getMessage());
            }
        } else {
            tokens = deviceFlow(session);
        }
        session.saveTokens(tokens);
        System.out.println("Logged in to " + endpoints.issuer);
        return 0;
    }

    /** RFC 8628: show the code, then poll the token endpoint until the user has approved. */
    private static Tokens deviceFlow(Session session) {
        JsonNode device = session.authorizeDevice();
        String url = device.path("verification_uri_complete").asText(device.path("verification_uri").asText());
        System.out.println("Open " + url);
        System.out.println("and confirm the code " + device.path("user_code").asText());
        OidcClient client = session.client(OidcClientConfig.Grant.Type.DEVICE, Map.of());
        Map<String, String> deviceCode = Map.of("device_code", device.path("device_code").asText());
        Duration interval = Duration.ofSeconds(device.path("interval").asLong(5));
        Instant deadline = Instant.now().plusSeconds(device.path("expires_in").asLong(600));
        while (Instant.now().isBefore(deadline)) {
            try {
                Thread.sleep(interval);
                return session.await(client.getTokens(deviceCode));
            } catch (OidcClientException e) {
                String error = String.valueOf(e.getMessage());
                if (error.contains("slow_down")) {
                    interval = interval.plusSeconds(5);
                } else if (!error.contains("authorization_pending")) {
                    throw new CliException("login failed: " + error);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CliException("interrupted");
            }
        }
        throw new CliException("login timed out");
    }
}
