package org.streamhouseoss.cli;

import java.util.concurrent.Callable;

import picocli.CommandLine.Mixin;

/** Shared endpoint options and error handling: CLI errors print a message and exit with 1. */
abstract class BaseCommand implements Callable<Integer> {

    @Mixin
    Endpoints endpoints;

    Session session() {
        return new Session(endpoints);
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
