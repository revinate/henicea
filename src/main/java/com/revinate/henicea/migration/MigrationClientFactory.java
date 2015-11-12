package com.revinate.henicea.migration;

import com.datastax.driver.core.Session;

public interface MigrationClientFactory {

    MigrationClient newClient(Session session, String keyspace, String uniqueClientId);
    MigrationClient newClient(Session session, String keyspace, String uniqueClientId, int replicationFactor);

}
