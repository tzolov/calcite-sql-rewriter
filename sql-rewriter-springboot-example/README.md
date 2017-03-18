# SQL Rewriter SpringBoot Sample

This project shows how to use the `sql-rewriter` within SpringBoot application. It leverages `JcbcTemplate` and the 
configuration properties support. 
 
The `SqlRewriterConfiguration` class inlines the `sql-rewriter`'s model.json configuration files into 
a plain Spring `application.properties` (or YMAL) configuration. Furthermore you can splint the those properties files 
as required and leverage the Spring profiles. 

#### How To Use

1. Create a `hr.depts_journal` table on a postgres (or HAWQ) server as configured in [application-default.properties](src/main/resources/application-default.properties).

```sql
DROP SCHEMA IF EXISTS hr CASCADE;
CREATE SCHEMA hr;
CREATE TABLE ${ACTUAL_SCHEMA}.depts_journal (
  deptno                    SERIAL                   NOT NULL,
  department_name           TEXT                     NULL     DEFAULT NULL, -- Nullable test column
  version_number            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  subsequent_version_number TIMESTAMP WITH TIME ZONE NULL     DEFAULT NULL,
  PRIMARY KEY (deptno, version_number)
);

```
2. Withint `sql-rewriter-springboot-example` folder compile the and run the project
```bash
cd sql-rewriter-springboot-example/
mvn clean install
```
```bash
java -jar target/sql-rewriter-springboot-example-0.0.1-SNAPSHOT.jar
```
You will see out put like this:
```bash
: --- CREATE depts_journal table --- 
: --- INSERT in depts table --- 
: --- QUERY all depts --- 
:       Department{deptno=666, departmentName='Department666'}
:       Department{deptno=667, departmentName='Department667'}
:       Department{deptno=668, departmentName='Department668'}
: --- UPDATE depts table --- 
: --- QUERY all depts --- 
:       Department{deptno=666, departmentName='NewName'}
:       Department{deptno=667, departmentName='NewName'}
:       Department{deptno=668, departmentName='NewName'}
: --- DELETE depts table rows --- 
: --- QUERY all depts --- 
: --- INSERT in depts table --- 
: --- QUERY all depts --- 
:       Department{deptno=666, departmentName='Department666'}
:       Department{deptno=667, departmentName='Department667'}
:       Department{deptno=668, departmentName='Department668'}
: --- QUERY all depts_journal --- 
:       ver: [2017-03-18 08:14:16.985884+01] sub_ver: [null] deptsno: [666] depts_name: [Department666]
:       ver: [2017-03-18 08:14:17.030069+01] sub_ver: [null] deptsno: [667] depts_name: [Department667]
:       ver: [2017-03-18 08:14:17.053779+01] sub_ver: [null] deptsno: [668] depts_name: [Department668]
:       ver: [2017-03-18 08:14:17.775103+01] sub_ver: [null] deptsno: [666] depts_name: [NewName]
:       ver: [2017-03-18 08:14:18.110022+01] sub_ver: [null] deptsno: [667] depts_name: [NewName]
:       ver: [2017-03-18 08:14:18.328976+01] sub_ver: [null] deptsno: [668] depts_name: [NewName]
:       ver: [2017-03-18 08:14:18.392501+01] sub_ver: [2017-03-18 08:14:18.392501+01] deptsno: [666] depts_name: [NewName]
:       ver: [2017-03-18 08:14:18.42414+01 ] sub_ver: [2017-03-18 08:14:18.42414+01] deptsno: [667] depts_name: [NewName]
:       ver: [2017-03-18 08:14:18.452851+01] sub_ver: [2017-03-18 08:14:18.452851+01] deptsno: [668] depts_name: [NewName]
:       ver: [2017-03-18 08:14:18.485342+01] sub_ver: [null] deptsno: [666] depts_name: [Department666]
:       ver: [2017-03-18 08:14:18.497312+01] sub_ver: [null] deptsno: [667] depts_name: [Department667]
:       ver: [2017-03-18 08:14:18.511399+01] sub_ver: [null] deptsno: [668] depts_name: [Department668]
: --- END --- 

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
$ java -jar myproject.jar --spring.config.location=classpath:/default.properties,classpath:/override.properties
```
Use `spring.config.name` environment property to switch from `application.properties` to another name.


#### [Spring Profile-specific properties](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html#boot-features-external-config-profile-specific-properties)
In addition to `application.properties` files, profile-specific properties can also be defined using the naming convention `application-{profile}.properties`.
he `Environment` has a set of default profiles (by default `[default]`) which are used if no active profiles are set (i.e. if no profiles are explicitly activated then properties from `application-default.properties` are loaded).

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
make sure you select the latest version: [ ![Download](https://api.bintray.com/packages/big-data/maven/calcite-sql-rewriter/images/download.svg) ](https://bintray.com/big-data/maven/calcite-sql-rewriter/_latestVersion)

