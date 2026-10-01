package org.streamhouseoss.controlplane.clients;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.streamhouseoss.model.OptionValue;

class SecretsTest {

    private final Secrets secrets = new Secrets(Map.of("STREAMHOUSE_SECRET_SHOP_PG_PWD", "s3cret")::get);

    @Test
    void resolvesLiteralsAndSecretsFromTheEnvironment() {
        assertThat(secrets.resolve(new OptionValue.Literal("shop-db"))).isEqualTo("shop-db");
        assertThat(secrets.resolve(new OptionValue.Secret("shop_pg_pwd"))).isEqualTo("s3cret");
        assertThat(Secrets.variable("crm.db-password")).isEqualTo("STREAMHOUSE_SECRET_CRM_DB_PASSWORD");
    }

    @Test
    void missingSecretsSayWhichVariableToSet() {
        assertThatThrownBy(() -> secrets.resolve(new OptionValue.Secret("crm_pwd")))
                .hasMessage("secret 'crm_pwd' is not set; define STREAMHOUSE_SECRET_CRM_PWD in the control plane's environment");
    }
}
