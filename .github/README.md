# Zula Queue Package

Single RabbitMQ queue package for Zula microservices.

## Available Package

- `com.zula:zula-queue-package`

## Publishing

```bash
mvn clean deploy
```

## Consumption

```xml
<repositories>
  <repository>
    <id>github</id>
    <name>GitHub Packages</name>
    <url>https://maven.pkg.github.com/Zula-Z/zula-queue-package</url>
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
