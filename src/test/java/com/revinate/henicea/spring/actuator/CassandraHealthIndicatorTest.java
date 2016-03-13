package com.revinate.henicea.spring.actuator;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;
import static org.assertj.core.util.Lists.newArrayList;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CassandraHealthIndicatorTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    Session session;

    @InjectMocks
    CassandraHealthIndicator indicator;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    Host host;

    @Before
    public void setUp() throws Exception {
        when(session.getState().getConnectedHosts()).thenReturn(newArrayList(host));
        when(host.getAddress().getHostName()).thenReturn("server");
    }

    @Test
    public void health_shouldBeUp_whenThereAreAvailableConnections() throws Exception {
        when(session.getCluster().getMetrics().getOpenConnections().getValue()).thenReturn(2);
        when(host.getState()).thenReturn("UP");

        Health health = indicator.health();

        assertThat(health).isNotNull();
        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails())
                .contains(entry("openConnections", 2))
                .contains(entry("servers", ImmutableMap.of("server", "UP")));
    }

    @Test
    public void health_shouldBeDown_whenThereAreNoneAvailableConnections() throws Exception {
        when(session.getCluster().getMetrics().getOpenConnections().getValue()).thenReturn(0);
        when(host.getState()).thenReturn("DOWN");

        Health health = indicator.health();

        assertThat(health).isNotNull();
        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails())
                .contains(entry("openConnections", 0))
                .contains(entry("servers", ImmutableMap.of("server", "DOWN")));
    }
}