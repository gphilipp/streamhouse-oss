package org.streamhouseoss.cli;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/**
 * The OAuth 2.0 device authorization endpoint (RFC 8628) of a Keycloak realm. quarkus-oidc-client
 * performs the device-code token request, but not this first step.
 */
@Path("/protocol/openid-connect/auth/device")
public interface DeviceAuthorizationApi {

    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_JSON)
    JsonNode authorize(@FormParam("client_id") String clientId, @FormParam("scope") String scope);
}
