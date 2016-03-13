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

/**
 * Main class to execute cassandra migrations. This class does not depend on any Spring bean or other Spring runtime
 * object.
 *
 * <p>You can instantiate at any point of the initialization process. The most convenient time to run the migration
 * is after the {@link Cluster} is instantiated and before the {@link Session} is created. You usually want to use the
 * session with some keyspace and therefore you want the keyspace to be created before it. This sample config works in
 * majority of setups:
 *
 * <pre>
 * &#064;Configuration
 * &#064;Slf4j
 * public class CassandraConfig {
 *
 *     &#064;Autowired
 *     private ResourcePatternResolver resourceResolver;
 *
 *     &#064;Autowired
 *     private Environment environment;
 *
 *     &#064;Bean
 *     public Migrator migrator() {
 *         return new Migrator();
 *     }
 *
 *     &#064;Bean
 *     public Cluster cluster() throws IOException {
 *         Cluster cluster = Cluster.builder()
 *                 .addContactPoints(environment.getProperty("cassandra.contactPoints").split(","))
 *                 .withPort(environment.getProperty("cassandra.port", Integer.class))
 *                 .build();
 *
 *         log.info("Running migrations");
 *         migrator().execute(cluster, environment.getProperty("cassandra.keyspace"), getMigrations());
 *
 *         return cluster;
 *     }
 *
 *     &#064;Bean
 *     public Session session() throws Exception {
 *         return cluster().connect(environment.getProperty("cassandra.keyspace"));
 *     }
 *
 *     private Resource[] getMigrations() throws IOException {
 *         return resourceResolver.getResources("classpath:/cassandra/*.cql");
 *     }
 * }
 * </pre>
 *
 * <p>The migration will create the keyspace with SimpleStrategy if the desired keyspace does not exists. This is very
 * useful for local development. If you are using Docker you do not need much more setup. For production you probably
 * want to have your keyspace created upfront with the right strategy, security and replication factory. Let this
 * library deal with tables and types only.
 */
@Slf4j
public class Migrator {

    private Optional<Integer> replicationFactor = Optional.empty();

    @Setter
    private MigrationClientFactory factory = DefaultMigrationClient::new;

    /**
     * By default the migrations are sorted by filename. This is a simplistic pattern but it does not scale. Instead of
     * making any assumptions about the file name format, you are free to create yours and set the sorting strategy
     * here.
     */
    @Setter
    private Comparator<Resource> resourceComparator = comparing(Resource::getFilename);

    /**
     * Main method to execute the migration.
     *
     * @param cluster  A properly initialized {@link Cluster}
     * @param keyspace Cassandra' keyspace/column family
     * @param resource An array of Spring {@link Resource} of migration files. Use Spring's
     *                 {@link org.springframework.core.io.support.ResourcePatternResolver} to load the migrations files.
     */
    public void execute(Cluster cluster, String keyspace, Resource... resource) {
        try (Session session = cluster.connect()) {
            MigrationClient client = factory.newClient(session, keyspace,
                    getHostname().orElseGet(() -> UUID.randomUUID().toString()));

            log.debug("Initializing cassandra schema");
            client.init(replicationFactor);

            log.debug("Getting lease to apply migrations");
            runWithLock(client, (appliedMigrations) -> parseMigrations(resource)
                    .filter(wasAppliedWith(appliedMigrations).negate())
                    .forEach(client::runMigration));
        }
    }

    /**
     * Set the replication factor if you want this migration to create the keyspace with SimpleStrategy. If the
     * keyspace already exists then this step will be ignored.
     *
     * @param replicationFactor The replication factor for
     */
    public void setReplicationFactor(Integer replicationFactor) {
        this.replicationFactor = Optional.ofNullable(replicationFactor);
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
