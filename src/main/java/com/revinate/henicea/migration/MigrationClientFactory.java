package com.revinate.henicea.migration;

import com.datastax.driver.core.Session;

@FunctionalInterface
public interface MigrationClientFactory {

    MigrationClient newClient(Session session, String keyspace, String uniqueClientId);

}
