package com.revinate.henicea.migration;

import java.util.Optional;
import java.util.SortedSet;

public interface MigrationClient {

    void init(Optional<Integer> replicationFactor);

    boolean acquireLock();

    void releaseLock();

    SortedSet<String> getAppliedMigrations();

    void runMigration(Migration migration);
}
