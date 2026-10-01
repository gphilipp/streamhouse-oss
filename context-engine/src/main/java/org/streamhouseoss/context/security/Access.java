package org.streamhouseoss.context.security;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.streamhouseoss.context.ContextConfig;
import org.streamhouseoss.context.store.ServingStore;
import org.streamhouseoss.context.store.TableInfo;

import io.quarkus.security.identity.SecurityIdentity;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Who may query what. Super roles (admin, engineer by default) see every table; everyone else
 * needs a {@code GRANT SELECT ON CONTEXT <table> TO ROLE <role>} for one of their roles.
 */
@ApplicationScoped
public class Access {

    private final ServingStore store;
    private final Set<String> superRoles;

    public Access(ServingStore store, ContextConfig config) {
        this.store = store;
        this.superRoles = config.superRoles();
    }

    public record Caller(String principal, Set<String> roles) {
        public static Caller of(SecurityIdentity identity) {
            String principal = identity.isAnonymous() ? "anonymous" : identity.getPrincipal().getName();
            return new Caller(principal, Set.copyOf(identity.getRoles()));
        }
    }

    /** Tables the caller may see and query. */
    public List<TableInfo> visibleTables(Caller caller) {
        List<TableInfo> tables = store.tables();
        if (caller.roles().stream().anyMatch(superRoles::contains)) {
            return tables;
        }
        Set<String> granted = store.grantedTopics(caller.roles());
        return tables.stream().filter(t -> granted.contains(t.topic())).collect(Collectors.toList());
    }
}
