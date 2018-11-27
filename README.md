# Henicea
[![Build Status](https://travis-ci.org/revinate/henicea.svg?branch=master)](https://travis-ci.org/revinate/henicea)
[![codecov.io](https://codecov.io/github/revinate/henicea/coverage.svg?branch=master)](https://codecov.io/github/revinate/henicea?branch=master)
[![Codacy Badge](https://api.codacy.com/project/badge/grade/88a5f92bb3d449df8418d22bb4319e51)](https://www.codacy.com/app/jrglee/henicea)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.revinate/henicea/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.revinate/henicea)
[![Javadocs](http://www.javadoc.io/badge/com.revinate/henicea.svg)](http://www.javadoc.io/doc/com.revinate/henicea)

Henicea was the sister of Cassandra in the Greek Mythology.

## Migration

Henicea provides a simple migration mechanism for Java projects.

Example config class for Spring Boot:

CassandraConfig.java
```java
@Configuration
@Slf4j
public class CassandraConfig {

    @Autowired
    private ResourcePatternResolver resourceResolver;

    @Autowired
    private Environment environment;

    @Bean
    public Migrator migrator() {
        return new Migrator();
    }

    @Bean
    public Cluster cluster() throws IOException {
        Cluster cluster = Cluster.builder()
                .addContactPoints(environment.getProperty("cassandra.contactPoints").split(","))
                .withPort(environment.getProperty("cassandra.port", Integer.class))
                .build();

        log.info("Running migrations");
        migrator().execute(cluster, environment.getProperty("cassandra.keyspace"), getMigrations());

        return cluster;
    }

    @Bean
    public Session session() throws Exception {
        return cluster().connect(environment.getProperty("cassandra.keyspace"));
    }

    private Resource[] getMigrations() throws IOException {
        return resourceResolver.getResources("classpath:/cassandra/*.cql");
    }
}
```

application.properties
```
cassandra.contactPoints=cassandra-node1,cassandra-node2
cassandra.port=9042
cassandra.keyspace=myapp
```

### Migration files

The migration file can be named in any pattern. By default the files are sorted by name
but the `resourceComparator` in the `Migrator` class has a setter to allow custom strategies.

Due to limitations in the driver, each migration file can have only **one** statement.

## Waiting for Cassandra to start

This is a common situation when you use Docker for local development and functional tests,
the Cassandra database will start in a separate container and it may take longer to get ready
than your application. This library comes with a simple utility to help with retry and
exponential back-off.

```java
Cluster cluster = new Retryer()
        .onError((attempt, exception) -> log.warn("Could not connect to cassandra cluster on attempt number {}. Wrapped exception", attempt, exception))
        .withWait(TimeUnit.SECONDS, 1, 2, 4, 8, 10, 10)
        .run(() -> {
            Cluster candidate = Cluster.builder()
                    .addContactPoints("localhost")
                    .withPort(9042)
                    .build();

            candidate.init(); // will throw exception if none of the contact points can be reached

            return candidate;
        });
```

The `run` method will throw the last thrown exception if all the attempts failed.

## Health check

Henicea provides a simple health check through Spring Boot Actuator. The only requirement
is to have a Cassandra `Session` object in the Spring context.

To auto configure a health check add
```java
@Bean
public CassandraHealthIndicator cassandraHealthIndicator() {
    return new CassandraHealthIndicator();
}
```
to your Java config class.