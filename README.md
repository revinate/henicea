# Henicea

Henicea was the brother of Cassandra in the Greek Mythology.

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