package org.streamhouseoss.model;

/**
 * A connection option. Secrets are never stored in clear text in the desired state: only a
 * reference is kept and resolved by the control plane when it renders external configuration.
 */
public sealed interface OptionValue {

    record Literal(String value) implements OptionValue {
    }

    record Secret(String ref) implements OptionValue {
    }
}
