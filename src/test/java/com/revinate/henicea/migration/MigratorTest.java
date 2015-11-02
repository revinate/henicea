package com.revinate.henicea.migration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.io.Resource;

import java.io.ByteArrayInputStream;
import java.util.SortedSet;
import java.util.TreeSet;

import static java.util.Collections.emptySortedSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MigratorTest {

    @Mock
    Cluster cluster;

    @Mock
    MigrationClient client;

    @Mock
    Session session;

    Migrator migrator;

    @Before
    public void setUp() throws Exception {
        migrator = new Migrator();
        migrator.setFactory((session, keyspace, uniqueClientId) -> client);

        when(cluster.connect()).thenReturn(session);
    }

    @Test
    public void execute_shouldCloseOpenedSessions() throws Exception {
        when(client.acquireLock()).thenReturn(false);

        migrator.execute(cluster, "test");

        verify(client, times(1)).init();
        verify(session, times(1)).close();
    }

    @Test
    public void execute_shouldRunMigrationsWhenLockWasAcquired() throws Exception {
        Resource resource = mock(Resource.class);

        when(client.acquireLock()).thenReturn(true);
        when(client.getAppliedMigrations()).thenReturn(emptySortedSet());
        when(resource.exists()).thenReturn(true);
        when(resource.getFilename()).thenReturn("001_initial_migration.cql");
        when(resource.getInputStream()).thenReturn(new ByteArrayInputStream("create table foo (id uuid PRIMARY KEY)".getBytes()));

        migrator.execute(cluster, "test", resource);

        ArgumentCaptor<Migration> captor = ArgumentCaptor.forClass(Migration.class);
        verify(client).runMigration(captor.capture());
        assertThat(captor.getAllValues())
                .hasSize(1).contains(new Migration("001_initial_migration.cql", "create table foo (id uuid PRIMARY KEY)"));

        verify(client, times(1)).releaseLock();
    }

    @Test
    public void execute_shouldSkipMigrationsWhenLockWasNotAcquired() throws Exception {
        Resource resource = mock(Resource.class);

        when(client.acquireLock()).thenReturn(false);

        migrator.execute(cluster, "test", resource);

        verify(client, never()).runMigration(any());
        verify(client, never()).releaseLock();
    }

    @Test
    public void execute_shouldIgnoreAppliedMigrations() throws Exception {
        Resource resource = mock(Resource.class);

        SortedSet<String> appliedMigrations = new TreeSet<>();
        appliedMigrations.add("001_initial_migration.cql");

        when(client.acquireLock()).thenReturn(true);
        when(client.getAppliedMigrations()).thenReturn(appliedMigrations);
        when(resource.exists()).thenReturn(true);
        when(resource.getFilename()).thenReturn("001_initial_migration.cql");
        when(resource.getInputStream()).thenReturn(new ByteArrayInputStream("create table foo (id uuid PRIMARY KEY)".getBytes()));

        migrator.execute(cluster, "test", resource);

        verify(client, never()).runMigration(any());

        verify(client, times(1)).releaseLock();
    }
}