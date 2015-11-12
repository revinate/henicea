package com.revinate.henicea.migration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;

@Slf4j
public class Migrator {

    @Setter
    private MigrationClientFactory factory = new MigrationClientFactoryImpl();

    @Setter
    private Comparator<Resource> resourceComparator = comparing(Resource::getFilename);

    public void execute(Cluster cluster, String keyspace, Resource... resource) {
        try (Session session = cluster.connect()) {
            MigrationClient client = factory.newClient(session, keyspace,
                    getHostname().orElseGet(() -> UUID.randomUUID().toString()));
            doExecute(client, resource);
        }
    }

    public void execute(Cluster cluster, String keyspace, int replicationFactor, Resource... resource) {
        try (Session session = cluster.connect()) {
            MigrationClient client = factory.newClient(session, keyspace,
                    getHostname().orElseGet(() -> UUID.randomUUID().toString()), replicationFactor);
            doExecute(client, resource);
        }
    }

    private void doExecute(MigrationClient client, Resource... resource) {
        log.debug("Initializing cassandra schema");
        client.init();

        log.debug("Getting lease to apply migrations");
        runWithLock(client, (appliedMigrations) -> parseMigrations(resource)
                .filter(wasAppliedWith(appliedMigrations).negate())
                .forEach(client::runMigration));
    }

    private Stream<Migration> parseMigrations(Resource... resource) {
        return Stream.of(resource)
                .filter(Resource::exists)
                .sorted(resourceComparator)
                .map(Migration::fromResource)
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    private void runWithLock(MigrationClient client, Consumer<SortedSet<String>> consumer) {
        if (client.acquireLock()) {
            try {
                consumer.accept(client.getAppliedMigrations());
            } catch (Exception e) {
                log.error("Error applying migrations", e);
                throw e;
            } finally {
                client.releaseLock();
            }
        }
    }

    private static Optional<String> getHostname() {
        try {
            return Optional.ofNullable(InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            return Optional.empty();
        }
    }

    private static Predicate<Migration> wasAppliedWith(Set<String> appliedMigrations) {
        return migration -> {
            if (appliedMigrations.contains(migration.getName())) {
                log.debug("Skipping applied migration {}", migration.getName());
                return true;
            } else {
                return false;
            }
        };
    }
}
