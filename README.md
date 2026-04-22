# zula-queue-package

Single RabbitMQ queue package for Zula microservices.

## Dependency

All queue support is published as one Maven package:

```xml
<repositories>
  <repository>
    <id>github</id>
    <name>GitHub Packages</name>
    <url>https://maven.pkg.github.com/Zula-Z/zula-queue-package</url>
  </repository>
  <repository>
    <id>github</id>
    <name>GitHub Packages</name>
    <url>https://maven.pkg.github.com/Zula-Z/zula-database-package</url>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>com.zula</groupId>
    <artifactId>zula-queue-package</artifactId>
    <version>VERSION</version>
  </dependency>
</dependencies>
```

The queue package depends on the single database package:

```xml
<dependency>
  <groupId>com.zula</groupId>
  <artifactId>zula-database-package</artifactId>
  <version>DATABASE_VERSION</version>
</dependency>
```

The queue package version and the database dependency version are controlled only in the root `pom.xml`.

PostgreSQL remains the default through `zula-database-package`. For MySQL/MS services, configure the database package the same way as other consumers:

```yaml
spring:
  datasource:
    url: jdbc:mysql://localhost:3306/mydb
    driver-class-name: com.mysql.cj.jdbc.Driver

zula:
  database:
    provider: mysql
```

The queue package uses the same database-backed queue/DLQ persistence code and switches the DLQ registry DDL to MySQL-compatible auto-increment when the datasource is MySQL.

## Usage

Extend `BaseMessageConsumer<T>` and implement `processMessage(T message)`.

```java
@Service
public class AuthResponseConsumer extends BaseMessageConsumer<AuthResponseMessage> {
    public AuthResponseConsumer() {
        super("auth-response");
    }

    @Override
    public void processMessage(AuthResponseMessage message) {
        // handle message
    }
}
```

The base consumer registers a listener container when a RabbitMQ `ConnectionFactory` is available, so consuming applications do not need `@RabbitListener` queue expressions for the standard flow.

## Testkit

The single jar includes `com.zula.queue.testkit.RecordingQueueManager` for unit tests that need to verify queue creation without RabbitMQ:

```java
QueueProperties properties = new QueueProperties();
RecordingQueueManager queueManager = new RecordingQueueManager(properties);
queueManager.createServiceQueue("auth", "user-created");
```

The project test suite also verifies scan-package discovery and MySQL/MS DLQ DDL behavior.

## Build

```bash
mvn clean install
```

CI builds and publishes the root jar only, so GitHub Packages should contain one package: `com.zula:zula-queue-package`.
