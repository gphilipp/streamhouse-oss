package org.streamhouseoss.cli;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

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
        JsonNode tokens;
        if (username != null) {
            if (password == null) {
                throw new CliException("--password (or SHCTL_PASSWORD) is required with --username");
            }
            tokens = session.tokenRequest(Map.of("grant_type", "password", "client_id", Session.CLIENT_ID,
                    "username", username, "password", password, "scope", "openid"));
        } else {
            tokens = deviceFlow(session);
        }
        session.saveTokens(tokens);
        System.out.println("Logged in to " + endpoints.issuer);
        return 0;
    }

    private static JsonNode deviceFlow(Session session) {
        JsonNode device = session.deviceAuthorization();
        String url = device.path("verification_uri_complete").asText(device.path("verification_uri").asText());
        System.out.println("Open " + url);
        System.out.println("and confirm the code " + device.path("user_code").asText());
        Duration interval = Duration.ofSeconds(device.path("interval").asLong(5));
        Instant deadline = Instant.now().plusSeconds(device.path("expires_in").asLong(600));
        while (Instant.now().isBefore(deadline)) {
            try {
                Thread.sleep(interval);
                return session.tokenRequest(Map.of("grant_type", "urn:ietf:params:oauth:grant-type:device_code",
                        "client_id", Session.CLIENT_ID, "device_code", device.path("device_code").asText()));
            } catch (CliException e) {
                String error = e.body() == null ? "" : e.body().path("error").asText();
                if (error.equals("slow_down")) {
                    interval = interval.plusSeconds(5);
                } else if (!error.equals("authorization_pending")) {
                    throw e;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CliException("interrupted");
            }
        }
        throw new CliException("login timed out");
    }
}
