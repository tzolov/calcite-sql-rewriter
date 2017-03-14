# calcite-sql-rewriter
Converts `INSERT`, `UPDATE` and `DELETE` into append-only INSERT statements.
Useful for SQL-on-Hadoop like `Apache HAWQ` that doesn't provide in-place mutation operations yet.

### Overview
The `Sql-On-Hadoop` engines like Apache HAWQ often use `HDFS` (or alike) to store the data.
Later are design for high throughput scans. They does not allow random access or in-place data mutation.

Because of those limitations the `Sql-On-Hadoop` naturally support support append-only operations like `INSERT`
or operations that affect the entire data set like `DROP`.

In-place operations like `UPDATE` or `DELETE` require additional effort to 'emulate':
[HAWQ-304](https://issues.apache.org/jira/browse/HAWQ-304), [HIVE-5317](https://issues.apache.org/jira/browse/HIVE-5317)

This project provides a workaround that emulates the `INSERT`, `UPDATE` and `DELETE` operations by converting them into
append-only `INSERT`s. For example, instead of updating rows, insert the new version of the row using two
additional metadata columns: `version_number` and `subsequent_version_number`. The `version_number` holds the
version when the row was inserted. Latest `version_number` represents to the current record version.
The `subsequent_version_number` is set only when the row is marked as deleted, and can be populated on older records by
background archival tasks.

[Apache Calcite](https://calcite.apache.org/) is leveraged to implement this MMVC-like management.
Calcite exposes a `JDBC` connection and internally converts all incoming `INSERT`, `UPDATE` and `DELETE`
operations into append-only `INSERT`s as explained by the convention below.

For example given a business table `depts` with two columns `deptno` (key) and `department_name`:
```sql
CREATE TABLE hr.depts (
  deptno                    SERIAL                   NOT NULL,
  department_name           TEXT                     NOT NULL,
  PRIMARY KEY (deptno)
);
```
According to the convention you need to define a new `journal` table with name: `<your-table-name>_journal`) adding
two metadata columns: `version_number` and `subsequent_version_number` columns: (the column order does not matter)
```sql
CREATE TABLE hr.depts_journal (
  deptno                    SERIAL                   NOT NULL,
  version_number            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  subsequent_version_number TIMESTAMP WITH TIME ZONE NULL     DEFAULT NULL,
  department_name           TEXT                     NOT NULL,
  PRIMARY KEY (deptno, version_number)
);
```
Then
1. Issuing an `INSERT` statement against the Calcite JDBC driver
```sql
INSERT INTO hr.depts (deptno, department_name) VALUES (666, 'Pivotal');
```
is translated into SQL statement against the journal table:
```sql
INSERT INTO hr.depts_journal (deptno, department_name) VALUES (666, 'Pivotal');
```
_Note:_ that the table name is replaced by `<table-name>_journal`. The `hr.depts` may or may not exist. Data is
always goes to the `hr.depts_journal` table!

2. An `UPDATE` statement issued against the Calcite JDBC:
```sql
UPDATE hr.depts SET department_name='New Name' WHERE deptno = 666;
```
would be expanded into an `INSERT` statement like this:
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
3. A `DELETE` statement issued against the Calcite JDBC:
```sql
DELETE FROM hr.depts WHERE deptno=666;
```
would be expanded into an `INSERT` statement like this:
```sql
INSERT INTO hr.depts_journal (deptno, department_name, version_number, subsequent_version_number)
  WITH link_last AS (
      SELECT
        *,
        MAX(version_number) OVER (PARTITION BY deptno) AS last_version_number
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

4. A `SELECT` query against the Calcite JDBC connection:

```sql
SELECT * FROM hr.depts;
```
is converted into this `SELECT` statement:
```sql
WITH link_last AS (
    SELECT
      *,
      MAX(version_number) OVER (PARTITION BY deptno) AS last_version_number
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

### Limitations

When using this project, it is important to be aware of the following limitations:

* Concurrent updates to the same record can lead to data loss. If users A and B both send an update to the same record
  simultaneously, one of the users changes will be lost, even if they were updating different columns. Similarly, if one
  user deletes a record while another is updating it, the update may "win", causing the record to not be deleted.
* Unique indexes cannot be defined. Similarly, UPSERT (`ON CONFLICT UPDATE`) is not supported.

### References
* [HAWQ-304](https://issues.apache.org/jira/browse/HAWQ-304) Support update and delete on non-heap tables
* [HIVE-5317](https://issues.apache.org/jira/browse/HIVE-5317) Implement insert, update, and delete in Hive with full ACID support
* [HIVE - Insert/Update Implementation Design](https://issues.apache.org/jira/secure/attachment/12604051/InsertUpdatesinHive.pdf)
* [Working with Append-only Tables](http://it.toolbox.com/blogs/sap-on-db2/working-with-appendonly-tables-part-i-51352)
* [Four steps strategy for incremental updates in Hive](https://hortonworks.com/blog/four-step-strategy-incremental-updates-hive/)
