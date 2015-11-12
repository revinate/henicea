package com.revinate.henicea.migration;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static java.util.stream.Collectors.toCollection;

@RequiredArgsConstructor
@Slf4j
public class MigrationClient {

    private static final String KEYSPACE_CREATION_STATEMENT =
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}";
    private static final List<String> INIT_STATEMENTS = Arrays.asList(
            "CREATE TABLE IF NOT EXISTS %s.leases (name text PRIMARY KEY, owner text, value text) with default_time_to_live = 180",
            "CREATE TABLE IF NOT EXISTS %s.migrations (name text PRIMARY KEY, created_at timestamp, status text, statement text, reason text)"
    );

    private static final String LEASES_TABLE = "leases";
    private static final String MIGRATIONS_TABLE = "migrations";
    private static final String MIGRATION_LEASE_KEY = "migration";
    private static final int DEFAULT_REPLICATION_FACTOR = 1;

    private enum MigrationStatus {APPLYING, APPLIED, FAILED}

    private final Session session;
    private final String keyspace;
    private final String uniqueId;

    public void init(Optional<Integer> replicationFactor) {
        session.execute(String.format(KEYSPACE_CREATION_STATEMENT, keyspace,
                replicationFactor.orElse(DEFAULT_REPLICATION_FACTOR)));
        INIT_STATEMENTS.stream()
                .map(s -> String.format(s, keyspace))
                .forEach(session::execute);
    }

    public boolean acquireLock() {
        log.debug("Trying to acquire migration lock");
        ResultSet resultSet = session.execute(QueryBuilder
                .insertInto(keyspace, LEASES_TABLE)
                .value("name", MIGRATION_LEASE_KEY)
                .value("owner", uniqueId)
                .ifNotExists());

        boolean result = resultSet.wasApplied();
        log.debug("Migration lock acquired: {}", result);
        return result;
    }

    public void releaseLock() {
        log.debug("Releasing migration lock");
        session.execute(QueryBuilder
                .delete()
                .from(keyspace, LEASES_TABLE)
                .where(eq("name", MIGRATION_LEASE_KEY))
                .onlyIf(eq("owner", uniqueId)));
    }

    public SortedSet<String> getAppliedMigrations() {
        return session.execute(
                QueryBuilder
                        .select("name", "status")
                        .from(keyspace, MIGRATIONS_TABLE))
                .all()
                .stream()
                .filter(row -> MigrationStatus.APPLIED.name().equals(row.getString(1)))
                .map(row -> row.getString(0))
                .collect(toCollection(TreeSet::new));
    }

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

    private void addMigrationToTable(Migration migration) {
        session.execute(QueryBuilder
                .insertInto(keyspace, MIGRATIONS_TABLE)
                .value("name", migration.getName())
                .value("created_at", now())
                .value("status", MigrationStatus.APPLYING.name())
                .value("statement", migration.getStatement())
                .ifNotExists());
    }

    private void updateMigrationStatus(Migration migration, String status, Optional<String> reason) {
        session.execute(QueryBuilder
                .update(keyspace, MIGRATIONS_TABLE)
                .with(set("status", status))
                .and(set("reason", reason.orElse(null)))
                .where(eq("name", migration.getName()))
                .onlyIf(in("status", MigrationStatus.APPLYING.name(), MigrationStatus.FAILED.name())));
    }

    private static Object now() {
        return QueryBuilder.raw("dateOf(now())");
    }
}
