# SQL-Rewriter Simple Example

#### How To Use

1. Create a `hr.depts_journal` table on a postgres (or HAWQ) server as configured in [myTestConnection.json](src/main/resources/myTestConnection.json).

```sql
DROP SCHEMA IF EXISTS hr CASCADE;
CREATE SCHEMA hr;

CREATE TABLE hr.depts_journal (
  deptno                    SERIAL                   NOT NULL,
  version_number            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  subsequent_version_number TIMESTAMP WITH TIME ZONE NULL     DEFAULT NULL,
  department_name           TEXT                     NOT NULL,
  PRIMARY KEY (deptno, version_number)
);
```
2. From the top folder buid all projects:

```bash
mvn clean install -DskipTests
```

3. From the outher folder run the test:
```bash
java -jar ./journalled-sql-rewriter-example/target/journalled-sql-rewriter-example-1.6-SNAPSHOT.jar
```
if you are inside`journal-sql-rewriter-example/` then you need to provide the model type like this:
```bash
java -jar ./target/journalled-sql-rewriter-example-1.6-SNAPSHOT.jar src/main/resources/myTestModel.json
```

4. You should see an output like this:
```bash
 INSERT INTO hr.depts (deptno, department_name) VALUES(696, 'Pivotal')
   updated rows: 1
 SELECT * FROM hr.depts
   result: 696 , Pivotal , 
 UPDATE hr.depts SET department_name='interma' WHERE deptno = 696
   updated rows: 1
 SELECT * FROM hr.depts
   result: 696 , interma , 
 DELETE FROM hr.depts WHERE deptno = 696
   updated rows: 1
 SELECT * FROM hr.depts
   result:     
 Done				
```

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
	<version>1.6</version>
</dependency>

```
Always use the latest version: [ ![Download](https://api.bintray.com/packages/big-data/maven/calcite-sql-rewriter/images/download.svg) ](https://bintray.com/big-data/maven/calcite-sql-rewriter/_latestVersion)
