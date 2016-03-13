package com.revinate.henicea.migration;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Insert;
import org.assertj.core.api.Condition;
import org.hamcrest.CustomMatcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.atIndex;
import static org.assertj.core.api.Fail.fail;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MigrationClientTest {

    @Mock
    Session session;

    MigrationClient client;

    @Before
    public void setUp() throws Exception {
        client = new MigrationClient(session, "test", "unit-test-runner");
    }

    @Test
    public void init_shouldCreateBaseMigrationTablesWithDefaultRf() throws Exception {
        client.init(Optional.empty());

        ArgumentCaptor<String> statementCaptor = ArgumentCaptor.forClass(String.class);
        verify(session, atLeastOnce()).execute(statementCaptor.capture());
        assertThat(statementCaptor.getAllValues())
                .hasSize(3)
                .containsSequence(
                        "CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                        "CREATE TABLE IF NOT EXISTS test.leases (name text PRIMARY KEY, owner text, value text) with default_time_to_live = 180",
                        "CREATE TABLE IF NOT EXISTS test.migrations (name text PRIMARY KEY, created_at timestamp, status text, statement text, reason text)"
                );
    }

    @Test
    public void init_shouldCreateBaseMigrationTablesWithCustomRf() throws Exception {
        client.init(Optional.of(2));

        ArgumentCaptor<String> statementCaptor = ArgumentCaptor.forClass(String.class);
        verify(session, atLeastOnce()).execute(statementCaptor.capture());
        assertThat(statementCaptor.getAllValues())
                .hasSize(3)
                .contains("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
    }

    @Test
    public void acquireLock_shouldReturnTrueIfInsertSucceeds() throws Exception {
        ResultSet leaseResultSet = mock(ResultSet.class);

        when(session.execute(argThat(new CustomMatcher<Insert>("Get lease insert statement") {
            @Override
            public boolean matches(Object item) {
                String cql = item.toString();
                return cql.startsWith("INSERT INTO test.leases") && cql.contains("VALUES ('migration',");
            }
        }))).thenReturn(leaseResultSet);
        when(leaseResultSet.wasApplied()).thenReturn(true);

        assertThat(client.acquireLock()).isTrue();
    }

    @Test
    public void acquireLock_shouldReturnFalseIfInsertFails() throws Exception {
        ResultSet leaseResultSet = mock(ResultSet.class);

        when(session.execute(argThat(new CustomMatcher<Insert>("Get lease insert statement") {
            @Override
            public boolean matches(Object item) {
                String cql = item.toString();
                return cql.startsWith("INSERT INTO test.leases") && cql.contains("VALUES ('migration',");
            }
        }))).thenReturn(leaseResultSet);
        when(leaseResultSet.wasApplied()).thenReturn(false);

        assertThat(client.acquireLock()).isFalse();
    }

    @Test
    public void releaseLock_shouldDeleteLease() throws Exception {
        client.releaseLock();

        ArgumentCaptor<BuiltStatement> captor = ArgumentCaptor.forClass(BuiltStatement.class);
        verify(session, times(1)).execute(captor.capture());
        assertThat(captor.getAllValues())
                .hasSize(1)
                .extracting(BuiltStatement::toString)
                .containsOnly("DELETE FROM test.leases WHERE name='migration' IF owner='unit-test-runner';");
    }

    @Test
    public void getAppliedMigrations_shouldQueryAndSortByName() throws Exception {
        ResultSet appliedMigrationResultSet = mock(ResultSet.class);
        Row row = mock(Row.class);

        when(session.execute(argThat(new CustomMatcher<Insert>("Get applied migrations") {
            @Override
            public boolean matches(Object item) {
                return "SELECT name,status FROM test.migrations;".equals(item.toString());
            }
        }))).thenReturn(appliedMigrationResultSet);
        when(appliedMigrationResultSet.all()).thenReturn(Collections.singletonList(row));
        when(row.getString(0)).thenReturn("001_initial_migration.cql");
        when(row.getString(1)).thenReturn("APPLIED");

        assertThat(client.getAppliedMigrations()).containsOnly("001_initial_migration.cql");
    }

    @Test
    public void getAppliedMigrations_shouldIgnoreFailedAndIncompleteMigrations() throws Exception {
        ResultSet appliedMigrationResultSet = mock(ResultSet.class);
        Row row1 = mock(Row.class);
        Row row2 = mock(Row.class);

        when(session.execute(argThat(new CustomMatcher<Insert>("Get applied migrations") {
            @Override
            public boolean matches(Object item) {
                return "SELECT name,status FROM test.migrations;".equals(item.toString());
            }
        }))).thenReturn(appliedMigrationResultSet);
        when(appliedMigrationResultSet.all()).thenReturn(Arrays.asList(row1, row2));

        when(row1.getString(0)).thenReturn("001_initial_migration.cql");
        when(row1.getString(1)).thenReturn("APPLIED");
        when(row2.getString(0)).thenReturn("002_add_stuff.cql");
        when(row2.getString(1)).thenReturn("FAILED");

        assertThat(client.getAppliedMigrations()).containsOnly("001_initial_migration.cql");
    }

    @Test
    public void runMigration_shouldApplyMigrationAndLog() throws Exception {
        ResultSet migrationResultSet = mock(ResultSet.class);
        when(session.execute(anyString())).thenReturn(migrationResultSet);
        when(migrationResultSet.wasApplied()).thenReturn(true);

        client.runMigration(new Migration("001_initial_migration.cql", "create table foo (id uuid PRIMARY KEY)"));

        verify(session, times(1)).execute("create table foo (id uuid PRIMARY KEY)");

        ArgumentCaptor<BuiltStatement> captor = ArgumentCaptor.forClass(BuiltStatement.class);
        verify(session, times(2)).execute(captor.capture());
        assertThat(captor.getAllValues())
                .hasSize(2)
                .extracting(BuiltStatement::toString)
                .has(containsSubstr("INSERT INTO test.migrations"), atIndex(0))
                .has(containsSubstr("'001_initial_migration.cql',dateOf(now()),'APPLYING','create table foo (id uuid PRIMARY KEY)')"), atIndex(0))
                .has(containsSubstr("UPDATE test.migrations SET status='APPLIED'"), atIndex(1))
                .has(containsSubstr("IF status IN ('APPLYING','FAILED')"), atIndex(1));
    }

    @Test
    public void runMigration_shouldOverrideBrokenMigrations() throws Exception {
        ResultSet migrationResultSet = mock(ResultSet.class);
        when(session.execute(anyString())).thenReturn(migrationResultSet);
        when(migrationResultSet.wasApplied()).thenReturn(true);

        client.runMigration(new Migration("001_initial_migration.cql", "create table foo (id uuid PRIMARY KEY)"));

        verify(session, times(1)).execute("create table foo (id uuid PRIMARY KEY)");

        ArgumentCaptor<BuiltStatement> captor = ArgumentCaptor.forClass(BuiltStatement.class);
        verify(session, times(2)).execute(captor.capture());
        assertThat(captor.getAllValues())
                .hasSize(2)
                .extracting(BuiltStatement::toString)
                .has(containsSubstr("INSERT INTO test.migrations"), atIndex(0))
                .has(containsSubstr("IF NOT EXISTS"), atIndex(0))
                .has(containsSubstr("UPDATE test.migrations SET status='APPLIED'"), atIndex(1))
                .has(containsSubstr("IF status IN ('APPLYING','FAILED')"), atIndex(1));
    }

    @Test
    public void runMigration_shouldStopOnFailure() throws Exception {
        String invalidMigrationStatement = "invalid migration";

        when(session.execute(invalidMigrationStatement)).thenThrow(new InvalidQueryException("unit test"));

        try {
            client.runMigration(new Migration("001_initial_migration.cql", invalidMigrationStatement));
            fail("Excepted exception");
        } catch (InvalidQueryException ignored) {
        }

        verify(session, times(1)).execute(invalidMigrationStatement);

        ArgumentCaptor<BuiltStatement> captor = ArgumentCaptor.forClass(BuiltStatement.class);
        verify(session, times(2)).execute(captor.capture());
        assertThat(captor.getAllValues())
                .hasSize(2)
                .extracting(BuiltStatement::toString)
                .has(containsSubstr("UPDATE test.migrations SET status='FAILED'"), atIndex(1))
                .has(containsSubstr("reason='unit test'"), atIndex(1))
                .has(containsSubstr("IF status IN ('APPLYING','FAILED')"), atIndex(1));
    }

    private static Condition<String> containsSubstr(String expected) {
        return new Condition<>((String s) -> s.contains(expected), "Contains substr " + expected);
    }

}