# Zula Packages 🚀

Central package registry for the Zula microservices platform.

## 📦 Available Packages

- **zula-queue-library**: Automated RabbitMQ queue management for microservices

## 🔧 Setup

### For Publishing
1. Set GitHub token in Maven settings
2. Run: `mvn clean deploy`

### For Consumption
Add to your `pom.xml`:
```xml
<repositories>
    <repository>
        <id>github</id>
        <name>GitHub Packages</name>
        <url>https://maven.pkg.github.com/YOUR_USERNAME/zula-packages</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>com.zula</groupId>
        <artifactId>zula-queue-library</artifactId>
        <version>1.0.0</version>
    </dependency>
</dependencies>