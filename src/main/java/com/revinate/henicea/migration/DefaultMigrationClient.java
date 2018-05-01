package com.revinate.henicea.migration;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static java.util.stream.Collectors.toCollection;

/**
 * The default migration client in the library. The {@link Migrator} instantiated an instance of this client prior to
 * the execution. You can extends this class if necessary.
 */
@RequiredArgsConstructor
@Slf4j
public class DefaultMigrationClient implements MigrationClient {

    private static final String KEYSPACE_CREATION_STATEMENT =
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}";
    private static final String KEYSPACE_USE_STATEMENT = "USE %s";
    private static final List<String> INIT_STATEMENTS = Arrays.asList(
            "CREATE TABLE IF NOT EXISTS %s.leases (name text PRIMARY KEY, owner text, value text) with default_time_to_live = 180",
            "CREATE TABLE IF NOT EXISTS %s.migrations (name text PRIMARY KEY, created_at timestamp, status text, statement text, reason text)"
    );

    protected static final String LEASES_TABLE = "leases";
    protected static final String MIGRATIONS_TABLE = "migrations";
    protected static final String MIGRATION_LEASE_KEY = "migration";
    protected static final int DEFAULT_REPLICATION_FACTOR = 1;

    protected enum MigrationStatus {APPLYING, APPLIED, FAILED}

    protected final Session session;
    protected final String keyspace;
    protected final String uniqueId;

    /**
     * @param replicationFactor The optional replication factor when creating keyspace.
     */
    @Override
    public void init(Optional<Integer> replicationFactor) {
        session.execute(String.format(KEYSPACE_CREATION_STATEMENT, keyspace,
                replicationFactor.orElse(DEFAULT_REPLICATION_FACTOR)));
        session.execute(String.format(KEYSPACE_USE_STATEMENT, keyspace));
        INIT_STATEMENTS.stream()
                .map(s -> String.format(s, keyspace))
                .forEach(session::execute);
    }

    /**
     * Gets a lock by using paxos to insert into the <code>leases</code> table. The lock is used to prevent
     * multiple migrations from executing at the same time in a cluster deployment.
     *
     * @return true if the lock was acquired sucessfully.
     */
    @Override
    public boolean acquireLock() {
        log.debug("Trying to acquire migration lock");
        ResultSet resultSet = session.execute(insertInto(keyspace, LEASES_TABLE)
                .value("name", MIGRATION_LEASE_KEY)
                .value("owner", uniqueId)
                .ifNotExists());

        boolean result = resultSet.wasApplied();
        log.debug("Migration lock acquired: {}", result);
        return result;
    }

    /**
     * Release the lock acquired by {@link DefaultMigrationClient#acquireLock()}
     */
    @Override
    public void releaseLock() {
        log.debug("Releasing migration lock");
        session.execute(delete()
                .from(keyspace, LEASES_TABLE)
                .where(eq("name", MIGRATION_LEASE_KEY))
                .onlyIf(eq("owner", uniqueId)));
    }

    /**
     * Queries the <code>migrations</code> table for the migrations already applied.
     *
     * @return a sorted set of the migration files already applied.
     */
    @Override
    public SortedSet<String> getAppliedMigrations() {
        return session.execute(select("name", "status").from(keyspace, MIGRATIONS_TABLE))
                .all()
                .stream()
                .filter(row -> MigrationStatus.APPLIED.name().equals(row.getString(1)))
                .map(row -> row.getString(0))
                .collect(toCollection(TreeSet::new));
    }

    /**
     * Runs the actual migration. Rethrows any exception thrown by {@link Session#execute(String)}.
     *
     * @param migration The {@link Migration} to be applied.
     */
    @Override
    public void runMigration(Migration migration) {
        addMigrationToTable(migration);

        MigrationStatus status = MigrationStatus.APPLYING;
        Optional<String> reason = Optional.empty();

        try {
            ResultSet resultSet = session.execute(migration.getStatement());
            status = resultSet.wasApplied() ? MigrationStatus.APPLIED : MigrationStatus.FAILED;
        } catch (Exception e) {
            status = MigrationStatus.FAILED;
            reason = Optional.ofNullable(e.getMessage());
            log.error("Error applying migration {}", migration.getName(), e);
            throw e;
        } finally {
            updateMigrationStatus(migration, status.name(), reason);
            log.debug("{} executed with status {}", migration, status);
        }
    }

    protected void addMigrationToTable(Migration migration) {
        session.execute(insertInto(keyspace, MIGRATIONS_TABLE)
                .value("name", migration.getName())
                .value("created_at", now())
                .value("status", MigrationStatus.APPLYING.name())
                .value("statement", migration.getStatement())
                .ifNotExists());
    }

    protected void updateMigrationStatus(Migration migration, String status, Optional<String> reason) {
        session.execute(update(keyspace, MIGRATIONS_TABLE)
                .with(set("status", status))
                .and(set("reason", reason.orElse(null)))
                .where(eq("name", migration.getName()))
                .onlyIf(in("status", MigrationStatus.APPLYING.name(), MigrationStatus.FAILED.name())));
    }

    protected static Object now() {
        return raw("dateOf(now())");
    }
}
