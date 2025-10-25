# Clipper OFAC Spring Boot (Spring Boot 3, Java 17)

This is a starter Spring Boot 3 project that modernizes the legacy OFAC batch jobs into Spring Boot applications.

## Build

Install the IBM DB2 JDBC driver into your local or corporate Maven repository if needed (example command):
```
mvn install:install-file -Dfile=/path/to/db2jcc.jar -DgroupId=com.ibm.db2 -DartifactId=jcc -Dversion=11.5.9.0 -Dpackaging=jar
```

Then build:
```
mvn -DskipTests clean package
```

## Run

Example run (process job):
```
java -jar target/clipper-ofac-springboot-0.0.1-SNAPSHOT.jar DEV RESENDTRUE FULL 20250101
```

Primer job:
```
java -jar target/clipper-ofac-springboot-0.0.1-SNAPSHOT.jar DEV /tmp/prime.txt prime_data
```

## Config

`src/main/resources/application.yml` contains placeholders. Use environment variables or profiles for secrets.

-----------------------------

