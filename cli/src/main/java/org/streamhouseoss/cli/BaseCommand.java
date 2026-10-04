package org.streamhouseoss.cli;

import java.util.concurrent.Callable;

import io.quarkus.oidc.client.OidcClients;
import jakarta.inject.Inject;
import picocli.CommandLine.Mixin;

/** Shared endpoint options and error handling: CLI errors print a message and exit with 1. */
abstract class BaseCommand implements Callable<Integer> {

    @Mixin
    Endpoints endpoints;

    @Inject
    OidcClients oidc;

    Session session() {
        return new Session(endpoints, oidc);
    }

    @Override
    public final Integer call() {
        try {
            return run();
        } catch (CliException e) {
            System.err.println("error: " + e.getMessage());
            return 1;
        }
    }

    abstract int run();
}
