package org.streamhouseoss.cli;

import io.quarkus.picocli.runtime.annotations.TopCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@TopCommand
@Command(name = "shctl", mixinStandardHelpOptions = true, version = "shctl 0.1.0",
        description = "Streamhouse OSS command line: apply SQL, inspect resources, query real-time context.",
        subcommands = { LoginCommand.class, SqlCommand.class, GetCommand.class, DescribeCommand.class, QueryCommand.class,
                CommandLine.HelpCommand.class })
public class Shctl {
}
