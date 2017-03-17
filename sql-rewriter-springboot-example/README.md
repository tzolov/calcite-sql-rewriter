# SQL Rewriter Spring Boot Sample

You need to pre-create the following `hr.depts_journal` table on a Postgres\HAWQ running at: [application-default.properties](src/main/resources/application-default.properties)

```sql
DROP SCHEMA IF EXISTS hr CASCADE;
CREATE SCHEMA hr;
CREATE TABLE hr.depts_journal (
  deptno                    SERIAL          NOT NULL,
  department_name           TEXT            NULL     DEFAULT NULL, -- Nullable test column
  version_number            BIGINT NOT NULL DEFAULT 1,
  subsequent_version_number BIGINT NULL     DEFAULT NULL,
  PRIMARY KEY (deptno, version_number)
);

DELETE FROM hr.depts_journal;
```
##### [Srping Application property files](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html#boot-features-external-config-application-property-files)
SpringApplication will load properties from `application.properties` files in the following locations and add them to the Spring Environment:
* A `/config` subdirectory of the current directory.
* The current directory
* A classpath `/config` package
* The classpath root

Use `spring.config.location` environment property to provide alternative location for the property files (comma-separated list of directory locations, or file paths).
```bash
$ java -jar myproject.jar --spring.config.location=classpath:/default.properties,classpath:/override.properties
```
Use `spring.config.name` environment property to switch from `application.properties` to another name.


##### [Spring Profile-specific properties](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html#boot-features-external-config-profile-specific-properties)
In addition to `application.properties` files, profile-specific properties can also be defined using the naming convention `application-{profile}.properties`.
he `Environment` has a set of default profiles (by default `[default]`) which are used if no active profiles are set (i.e. if no profiles are explicitly activated then properties from `application-default.properties` are loaded).


