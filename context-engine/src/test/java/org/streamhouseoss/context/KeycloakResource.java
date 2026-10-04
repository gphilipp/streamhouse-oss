package org.streamhouseoss.context;

import java.time.Duration;
import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/** Keycloak with the platform's realm (deploy/compose/keycloak), with OIDC enabled in the app. */
public class KeycloakResource implements QuarkusTestResourceLifecycleManager {

    static GenericContainer<?> keycloak;

    static String realmUrl() {
        return "http://" + keycloak.getHost() + ":" + keycloak.getMappedPort(8080) + "/realms/streamhouse";
    }

    @Override
    public Map<String, String> start() {
        keycloak = new GenericContainer<>("keycloak/keycloak:26.8.0")
                .withCommand("start-dev", "--import-realm")
                .withEnv("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
                .withEnv("KC_BOOTSTRAP_ADMIN_PASSWORD", "admin")
                .withCopyFileToContainer(MountableFile.forHostPath("../deploy/compose/keycloak/streamhouse-realm.json"),
                        "/opt/keycloak/data/import/streamhouse-realm.json")
                .withExposedPorts(8080)
                .waitingFor(Wait.forHttp("/realms/streamhouse/.well-known/openid-configuration").forStatusCode(200))
                .withStartupTimeout(Duration.ofMinutes(3));
        keycloak.start();
        return Map.of(
                "quarkus.oidc.tenant-enabled", "true",
                "quarkus.oidc.auth-server-url", realmUrl(),
                "quarkus.oidc.token.issuer", realmUrl());
    }

    @Override
    public void stop() {
        if (keycloak != null) {
            keycloak.stop();
        }
    }
}
