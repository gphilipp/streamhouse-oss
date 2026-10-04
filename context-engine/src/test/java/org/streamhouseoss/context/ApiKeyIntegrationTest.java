package org.streamhouseoss.context;

import static io.restassured.RestAssured.given;

import org.junit.jupiter.api.Test;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * API keys against a real Keycloak: the key and secret of an OIDC client are exchanged for a token,
 * whose roles then apply exactly as for a bearer token.
 */
@QuarkusTest
@QuarkusTestResource(value = StackResource.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = KeycloakResource.class, restrictToAnnotatedClass = true)
class ApiKeyIntegrationTest {

    @Test
    void validKeyAuthenticatesWithTheClientsRoles() {
        // support-agent has the support_agent role: it may query, but not administer.
        given().auth().preemptive().basic("support-agent", "support-agent-secret").get("/v1/tables").then().statusCode(200);
        given().auth().preemptive().basic("support-agent", "support-agent-secret").get("/admin/v1/tables").then().statusCode(403);
        // control-plane has the admin role.
        given().auth().preemptive().basic("control-plane", "control-plane-secret").get("/admin/v1/tables").then().statusCode(200);
    }

    @Test
    void wrongSecretsAndMissingCredentialsAreRejected() {
        given().auth().preemptive().basic("support-agent", "wrong").get("/v1/tables").then().statusCode(401);
        given().auth().preemptive().basic("no-such-client", "x").get("/v1/tables").then().statusCode(401);
        given().get("/v1/tables").then().statusCode(401);
        // A wrong attempt does not poison the valid key.
        given().auth().preemptive().basic("support-agent", "support-agent-secret").get("/v1/tables").then().statusCode(200);
    }

    @Test
    void bearerTokensStillWork() {
        String token = given().formParam("grant_type", "client_credentials")
                .formParam("client_id", "support-agent").formParam("client_secret", "support-agent-secret")
                .post(KeycloakResource.realmUrl() + "/protocol/openid-connect/token")
                .then().statusCode(200).extract().path("access_token");
        given().auth().oauth2(token).get("/v1/tables").then().statusCode(200);
    }
}
