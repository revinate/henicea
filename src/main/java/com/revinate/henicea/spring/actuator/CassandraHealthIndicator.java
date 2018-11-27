package com.revinate.henicea.spring.actuator;

import com.datastax.driver.core.Session;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple health indicator for Spring Boot actuator. It shows the servers and open connections.
 */
@Slf4j
public class CassandraHealthIndicator extends AbstractHealthIndicator {

    @Autowired
    private Session session;

    @Override
    protected void doHealthCheck(Health.Builder builder) throws Exception {
        int openConnections = countOpenConnections();

        builder
                .status(openConnections == 0 ? Status.DOWN : Status.UP)
                .withDetail("servers", getHostStatus())
                .withDetail("openConnections", openConnections);
    }

    private Integer countOpenConnections() {
        return session.getCluster().getMetrics().getOpenConnections().getValue();
    }

    private Map<String, String> getHostStatus() {
        Map<String, String> statusMap = new HashMap<>();

        // use a local map obj instead of collect to allow address collisions
        // it can happen if the contact point is an A record
        session.getState().getConnectedHosts()
                .forEach(host -> statusMap.put(host.getAddress().getHostName(), host.getState()));

        return statusMap;
    }
}
