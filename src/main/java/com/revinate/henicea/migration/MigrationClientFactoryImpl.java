package com.revinate.henicea.migration;

import com.datastax.driver.core.Session;

public class MigrationClientFactoryImpl implements MigrationClientFactory {

    @Override
    public MigrationClient newClient(Session session, String keyspace, String uniqueClientId) {
        return new MigrationClient(session, keyspace, uniqueClientId);
    }

    @Override
    public MigrationClient newClient(Session session, String keyspace, String uniqueClientId, int replicationFactor) {
        return new MigrationClient(session, keyspace, uniqueClientId, replicationFactor);
    }
}
