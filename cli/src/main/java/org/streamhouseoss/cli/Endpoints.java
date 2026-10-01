package org.streamhouseoss.cli;

import picocli.CommandLine.Option;

/** Where the platform lives; defaults match the local docker compose stack. */
public class Endpoints {

    @Option(names = "--server", defaultValue = "${env:SHCTL_SERVER:-http://localhost:8080}",
            description = "Control plane URL (env SHCTL_SERVER, default ${DEFAULT-VALUE})")
    String server;

    @Option(names = "--context-engine", defaultValue = "${env:SHCTL_CONTEXT_ENGINE:-http://localhost:8082}",
            description = "Context engine URL (env SHCTL_CONTEXT_ENGINE, default ${DEFAULT-VALUE})")
    String contextEngine;

    @Option(names = "--issuer", defaultValue = "${env:SHCTL_ISSUER:-http://localhost:8180/realms/streamhouse}",
            description = "OIDC issuer (env SHCTL_ISSUER, default ${DEFAULT-VALUE})")
    String issuer;
}
