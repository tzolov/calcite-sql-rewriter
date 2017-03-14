# calcite-sql-rewriter
Converts `INSERT`, `UPDATE` and `DELETE` into append-only INSERT statements. 
Useful for SQL-on-Hadoop like `Apache HAWQ` that doesn't provide in-place mutation operations yet. 

### Overview
The `Sql-On-Hadoop` engines like Apache HAWQ often use `HDFS` (or alike) to store the data. 
Later are design for high throughput scans. They does not allow random access or in-place data mutation.

Because of those limitations the `Sql-On-Hadoop` naturally support support append-only operations like `INSERT` 
or operations that affect the entire data set like `DROP`. 

In-place operations like `UPDATE` or `DELETE` require additional effort to `emulate`: 
[HAWQ-304](https://issues.apache.org/jira/browse/HAWQ-304), [HIVE-5317](https://issues.apache.org/jira/browse/HIVE-5317)  

This project project provides a workaround that emulates `ISERT`, `UPDATE` and `DELETE` operations by convertying them into 
append-only `INSERT` and using two additional columns (per-table): `version_number` to keep the latest ROW version 
and `sequence_version_number` to mark if and when a row is deleted.

[Apache Calcite](https://calcite.apache.org/) is leveraged as `JDBC` proxy to convert the incoming `INSERT, UDATE and DELTE` 
operations into `append-only` `INSERTS. It hides the plumbing code. 

For example given a business table that should looks like this:

```sql
CREATE TABLE hr.depts (
  deptno                    SERIAL                   NOT NULL,
  department_name           TEXT                     NOT NULL,
  PRIMARY KEY (deptno)
);
```
You need to define a corresponding journal table (with name `<your-table-name>_journal`) with `version_number` and `subsequent_version_number` columns like this:

```sql
CREATE TABLE hr.depts_journal (
  deptno                    SERIAL                   NOT NULL,
  version_number            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  subsequent_version_number TIMESTAMP WITH TIME ZONE NULL     DEFAULT NULL,
  department_name           TEXT                     NOT NULL,
  PRIMARY KEY (deptno, version_number)
);
```
 
1. Following `INSERT` statement issued against the Calcite JDBC driver
```sql
INSERT INTO hr.depts (deptno, department_name) VALUES(666, 'Pivotal');
```
would be translated into SQL statement like this: 
```sql
INSERT INTO hr.depts_journal (deptno, department_name) VALUES (666, 'Pivotal');
```
_Note:_ that the table name is replaced by `<table-name>_journal`. The `hr.depts` may or may not exist. Data is 
always goes to the `hr.depts_journal` table!  

2. An `UPDATE` statements issued against the Calcite JDBC:
```sql
UPDATE hr.depts SET department_name='New Name' WHERE deptno = 666;
```
would be expanded into `INSERT` statement like this: 
```sql
INSERT INTO hr.depts_journal (deptno, department_name)
  WITH link_last AS (
      SELECT
        *,
        MAX(version_number) OVER (PARTITION BY deptno) AS last_version_number
      FROM hr.depts_journal
      WHERE deptno = 666
  )
  SELECT
    deptno,
    'New Name' as department_name
  FROM link_last
  WHERE subsequent_version_number IS NULL
        AND version_number = last_version_number;
```
3. An `DELETE` statements issued against the Calcite JDBC:
```sql
DELETE FROM hr.depts WHERE deptno=666;
```
would be expanded into `INSERT` statement like this:
```sql
INSERT INTO hr.depts_journal (deptno, department_name, version_number, subsequent_version_number)
  WITH link_last AS (
      SELECT
        *,
        MAX(version_number)
        OVER (PARTITION BY deptno) AS last_version_number
      FROM hr.depts_journal
      WHERE deptno = 666
  )
  SELECT
    deptno,
    department_name,
    CURRENT_TIMESTAMP AS version_number,
    CURRENT_TIMESTAMP AS subsequent_version_number
  FROM link_last
  WHERE subsequent_version_number IS NULL
        AND version_number = last_version_number;
```

4. `SELECT` query against the Calcite JDBC connection: 

```sql
SELECT * FROM hr.depts;
```
is converted this `SELECT` statement:
```sql
WITH link_last AS (
    SELECT
      *,
      MAX(version_number)
      OVER (PARTITION BY deptno) AS last_version_number
    FROM hr.depts_journal
)
SELECT
  deptno,
  department_name
FROM link_last
WHERE subsequent_version_number IS NULL
      AND version_number = last_version_number;
```
So for each `deptno` only the row with higher version_number is returned. 

---

_Note that this project provides just a temporal workaround. Complete solution will be provided with:_ [HAWQ-304](https://issues.apache.org/jira/browse/HAWQ-304) 

### References
* [HAWQ-304](https://issues.apache.org/jira/browse/HAWQ-304) Support update and delete on non-heap tables
* [HIVE-5317](https://issues.apache.org/jira/browse/HIVE-5317) Implement insert, update, and delete in Hive with full ACID support
* [HIVE - Insert/Update Implementation Design](https://issues.apache.org/jira/secure/attachment/12604051/InsertUpdatesinHive.pdf)
* [Working with Append-only Tables](http://it.toolbox.com/blogs/sap-on-db2/working-with-appendonly-tables-part-i-51352)
* [Four steps strategy for incremental updates in Hive](https://hortonworks.com/blog/four-step-strategy-incremental-updates-hive/)
