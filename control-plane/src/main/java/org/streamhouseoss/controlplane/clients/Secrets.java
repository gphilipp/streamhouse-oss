package org.streamhouseoss.controlplane.clients;

import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import org.streamhouseoss.model.OptionValue;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Resolves {@code SECRET 'name'} references. Milestone 1 reads environment variables named
 * {@code STREAMHOUSE_SECRET_<NAME>} so secrets never enter the desired state or the audit log.
 */
@ApplicationScoped
public class Secrets {

    private final Function<String, String> environment;

    public Secrets() {
        this(System::getenv);
    }

    Secrets(Function<String, String> environment) {
        this.environment = environment;
    }

    public String resolve(OptionValue value) {
        return switch (value) {
            case OptionValue.Literal literal -> literal.value();
            case OptionValue.Secret secret -> {
                String variable = variable(secret.ref());
                String resolved = environment.apply(variable);
                if (resolved == null) {
                    throw new ComponentException("secret '" + secret.ref() + "' is not set; define " + variable
                            + " in the control plane's environment");
                }
                yield resolved;
            }
        };
    }

    public String resolve(Map<String, OptionValue> options, String key) {
        OptionValue value = options.get(key);
        if (value == null) {
            throw new ComponentException("missing option '" + key + "'");
        }
        return resolve(value);
    }

    static String variable(String ref) {
        return "STREAMHOUSE_SECRET_" + ref.toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9]", "_");
    }
}
