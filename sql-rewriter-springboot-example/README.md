# SQL-Rewriter SpringBoot Example

How to use `sql-rewriter` within a [SpringBoot](https://projects.spring.io/spring-boot/) applications. Leverage
utilities such as [JdbcTemplate](http://docs.spring.io/spring-boot/docs/1.5.2.RELEASE/reference/htmlsingle/#boot-features-using-jdbc-template) 
and [еxternalized configuration](http://docs.spring.io/spring-boot/docs/1.5.2.RELEASE/reference/htmlsingle/#boot-features-external-config). 
 
* The [SqlRewriterConfiguration](src/main/java/io/pivotal/calcite/example/SqlRewriterConfiguration) class shows how to inline 
the `sql-rewriter` configuration files into plain Spring `application.properties` (or `YMAL`) configurations. 
* The [configuration options](#configuration-options) below explain how to use profiles so you can activate different 
configurations in different environments. 
* Resolve the `sql-rewriter` library from public [maven repo](#maven-repository). 

#### How To Use

1. Create a `hr.depts_journal` table on a postgres (or HAWQ) server as configured in [application-default.properties](src/main/resources/application-default.properties).

```sql
DROP SCHEMA IF EXISTS hr CASCADE;
CREATE SCHEMA hr;
CREATE TABLE hr.depts_journal (
  deptno                    SERIAL     NOT NULL,
  department_name           TEXT       NULL      DEFAULT NULL,
  version_number            SERIAL     NOT NULL,
  subsequent_version_number BIGINT     NULL      DEFAULT NULL,
  PRIMARY KEY (deptno, version_number)
);
```
_Mind that the `version_number` is of type `SERIAL` not `BIGINT`! This is important for the INSERT/DELETE/INSERT to work!_

2. Inside`sql-rewriter-springboot-example/` build the project
```bash
cd sql-rewriter-springboot-example/
mvn clean install
```
and the run it
```bash
java -jar target/sql-rewriter-springboot-example-0.0.1-SNAPSHOT.jar
```
_(use `--spring.profiles.active=<profile-name>` to activate the [configuration profile](#spring-profile-specific-properties) 
suitable for your environment)_

You should see an output like this:
```bash
 : 1. CREATE depts_journal table
 :      All depts:
 :        Count:0
 :      All depts_journal:
 :        Count:0
 : 2. INSERT 3 rows in depts
 :      All depts:
 :        Department{deptno=666, departmentName='Department666'}
 :        Department{deptno=667, departmentName='Department667'}
 :        Department{deptno=668, departmentName='Department668'}
 :        Count:3
 :      All depts_journal:
 :        ver: [1] sub_ver: [null] deptsno: [666] depts_name: [Department666]
 :        ver: [2] sub_ver: [null] deptsno: [667] depts_name: [Department667]
 :        ver: [3] sub_ver: [null] deptsno: [668] depts_name: [Department668]
 :        Count:3
 : 3. UPDATE all depts rows
 :      All depts:
 :        Department{deptno=666, departmentName='NewName'}
 :        Department{deptno=667, departmentName='NewName'}
 :        Department{deptno=668, departmentName='NewName'}
 :        Count:3
 :      All depts_journal:
 :        ver: [1] sub_ver: [null] deptsno: [666] depts_name: [Department666]
 :        ver: [2] sub_ver: [null] deptsno: [667] depts_name: [Department667]
 :        ver: [3] sub_ver: [null] deptsno: [668] depts_name: [Department668]
 :        ver: [2] sub_ver: [null] deptsno: [666] depts_name: [NewName]
 :        ver: [3] sub_ver: [null] deptsno: [667] depts_name: [NewName]
 :        ver: [4] sub_ver: [null] deptsno: [668] depts_name: [NewName]
 :        Count:6
 : 4. DELETE all depts rows
 :      All depts:
 :        Count:0
 :      All depts_journal:
 :        ver: [1] sub_ver: [null] deptsno: [666] depts_name: [Department666]
 :        ver: [2] sub_ver: [null] deptsno: [667] depts_name: [Department667]
 :        ver: [3] sub_ver: [null] deptsno: [668] depts_name: [Department668]
 :        ver: [2] sub_ver: [null] deptsno: [666] depts_name: [NewName]
 :        ver: [3] sub_ver: [null] deptsno: [667] depts_name: [NewName]
 :        ver: [4] sub_ver: [null] deptsno: [668] depts_name: [NewName]
 :        ver: [3] sub_ver: [3] deptsno: [666] depts_name: [NewName]
 :        ver: [4] sub_ver: [4] deptsno: [667] depts_name: [NewName]
 :        ver: [5] sub_ver: [5] deptsno: [668] depts_name: [NewName]
 :        Count:9
 : 5. INSERT 3 rows in depts again
 :      All depts:
 :        Department{deptno=666, departmentName='Second Insert 666'}
 :        Department{deptno=667, departmentName='Second Insert 667'}
 :        Department{deptno=668, departmentName='Second Insert 668'}
 :        Count:3
 :      All depts_journal:
 :        ver: [1] sub_ver: [null] deptsno: [666] depts_name: [Department666]
 :        ver: [2] sub_ver: [null] deptsno: [667] depts_name: [Department667]
 :        ver: [3] sub_ver: [null] deptsno: [668] depts_name: [Department668]
 :        ver: [2] sub_ver: [null] deptsno: [666] depts_name: [NewName]
 :        ver: [3] sub_ver: [null] deptsno: [667] depts_name: [NewName]
 :        ver: [4] sub_ver: [null] deptsno: [668] depts_name: [NewName]
 :        ver: [3] sub_ver: [3] deptsno: [666] depts_name: [NewName]
 :        ver: [4] sub_ver: [4] deptsno: [667] depts_name: [NewName]
 :        ver: [5] sub_ver: [5] deptsno: [668] depts_name: [NewName]
 :        ver: [4] sub_ver: [null] deptsno: [666] depts_name: [Second Insert 666]
 :        ver: [5] sub_ver: [null] deptsno: [667] depts_name: [Second Insert 667]
 :        ver: [6] sub_ver: [null] deptsno: [668] depts_name: [Second Insert 668]
 :        Count:12
 : 6. END						
```

## Configuration Options
#### [Srping Application property files](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html#boot-features-external-config-application-property-files)
SpringApplication will load properties from `application.properties` files in the following locations and add them to the Spring Environment:
* A `/config` subdirectory of the current directory.
* The current directory
* A classpath `/config` package
* The classpath root

Use `spring.config.location` environment property to provide alternative location for the property files (comma-separated list of directory locations, or file paths).
```bash
java -jar target/sql-rewriter-springboot-example-0.0.1-SNAPSHOT.jar --spring.config.location=classpath:/default.properties,classpath:/override.properties
```
Use `spring.config.name` environment property to switch from `application.properties` to another name.


#### [Spring Profile-specific properties](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html#boot-features-external-config-profile-specific-properties)
Profile-specific properties can also be defined using the naming convention `application-{profile}.properties`. 
The `[default]` is used if no active profiles are set (i.e. if no profiles are explicitly activated then properties 
from `application-default.properties` are loaded).

Use `spring.profiles.active` environment property to set the active property. For example:
```bash
java -jar target/sql-rewriter-springboot-example-0.0.1-SNAPSHOT.jar --spring.profiles.active=PROD
```
will load the`application-PROD.properties` configuration.
 
## Maven repository
To resolve `journalled-sql-rewriter` you need to add the following maven repository to your `pom.xml`  
```xml
<repository>
	<snapshots>
		<enabled>false</enabled>
	</snapshots>
	<id>bintray-big-data-maven</id>
	<name>bintray</name>
	<url>http://dl.bintray.com/big-data/maven</url>
</repository>
```
Then add then `journalled-sql-rewriter` dependency to your `pom.xml`
```xml
<dependency>
	<groupId>io.pivotal.calcite</groupId>
	<artifactId>journalled-sql-rewriter</artifactId>
	<version>1.4</version>
</dependency>

```
Always use the latest version: [ ![Download](https://api.bintray.com/packages/big-data/maven/calcite-sql-rewriter/images/download.svg) ](https://bintray.com/big-data/maven/calcite-sql-rewriter/_latestVersion)
