# calcite-sql-rewriter
Converts `INSERT`, `UPDATE` and `DELETE` into append-only INSERT statements.
Useful for SQL-on-Hadoop like `Apache HAWQ` that doesn't provide in-place mutation operations yet.
[ ![Download](https://api.bintray.com/packages/big-data/maven/calcite-sql-rewriter/images/download.svg) ](https://api.bintray.com/packages/big-data/maven/calcite-sql-rewriter/_latestVersion)

### Overview
The `Sql-On-Hadoop` engines like [Apache HAWQ](http://hawq.incubator.apache.org/) often use `HDFS` (or alike) to store the data.
Later are design for high throughput scans. They does not allow random access or in-place data mutation.

Because of those limitations the `Sql-On-Hadoop` naturally support support append-only operations like `INSERT`
or operations that affect the entire data set like `DROP`.

In-place operations like `UPDATE` or `DELETE` require additional effort to _emulate_:
[HAWQ-304](https://issues.apache.org/jira/browse/HAWQ-304), [HIVE-5317](https://issues.apache.org/jira/browse/HIVE-5317)

This project _emulates_ the `INSERT`, `UPDATE` and `DELETE` operations by converting them into
append-only `INSERT`s. Instead of updating rows in-place it inserts the new version of the row using two
additional metadata columns: `version_number` and `subsequent_version_number`. 

The `version_number` holds the version when the row was inserted in the table. The latest (or highest `version_number` 
represents the current record version. The `subsequent_version_number` is set only when the row is marked as deleted, 
and can be populated on older records by background archival tasks.

[Apache Calcite](https://calcite.apache.org/) is leveraged to implement the row version management. Externally it exposes a 
plain `JDBC` interface while internally it converts the incoming `INSERT`, `UPDATE` and `DELETE` statements into 
append-only `INSERT`s and sends them to pre-configured backend DB like [Apache HAWQ](http://hawq.incubator.apache.org/).

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
The `version_number` is generated on every row insert and holds the `CURRENT_TIMESTAP`. Latest `version_number` 
represents to the current row record.
The `subsequent_version_number` is set when the row is marked as deleted.

Note that the result key is composed of the busyness keys (`deptno`) and the version metadata (`version_number`). 

Here is how the `INSERT`, `UPDATE`, `DELETE` and `SELECT` statements are represented internally.

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
  SELECT
    deptno,
    'New Name' as department_name
  FROM (
    SELECT *, MAX(version_number) OVER (PARTITION BY deptno) AS last_version_number
    FROM hr.depts_journal    
  ) AS last_link
  WHERE subsequent_version_number IS NULL
        AND version_number = last_version_number
        AND deptno = 666;
```
3. A `DELETE` statement issued against the Calcite JDBC:
```sql
DELETE FROM hr.depts WHERE deptno=666;
```
would be expanded into an `INSERT` statement like this:
```sql
INSERT INTO hr.depts_journal (deptno, department_name, version_number, subsequent_version_number)
  SELECT
    deptno,
    department_name,
    CURRENT_TIMESTAMP AS version_number,
    CURRENT_TIMESTAMP AS subsequent_version_number
  FROM (
    SELECT *, MAX(version_number) OVER (PARTITION BY deptno) AS last_version_number
    FROM hr.depts_journal    
  ) AS last_link
  WHERE subsequent_version_number IS NULL
        AND version_number = last_version_number
        AND deptno = 666;
```

4. A `SELECT` query against the Calcite JDBC connection:

```sql
SELECT * FROM hr.depts;
```
is converted into this `SELECT` statement:
```sql
SELECT
  deptno,
  department_name
FROM (
  SELECT *, MAX(version_number) OVER (PARTITION BY deptno) AS last_version_number
  FROM hr.depts_journal
) AS link_last
WHERE subsequent_version_number IS NULL AND version_number = last_version_number;
```
For each unique `deptno` only the row with the higher `version_number` is returned. 

The `MAX(version_number) OVER (PARTITION BY deptno)` [window function](https://www.postgresql.org/docs/9.6/static/tutorial-window.html) computes the 
max `version_number` per `deptno`.

---
_Note that this project provides just a temporal workaround. Complete solution will be provided with:_ [HAWQ-304](https://issues.apache.org/jira/browse/HAWQ-304)

---

### Limitations

When using this project, it is important to be aware of the following limitations:

* Concurrent updates to the same record can lead to data loss. If users A and B both send an update to the same record
  simultaneously, one of the users changes will be lost, even if they were updating different columns. Similarly, if one
  user deletes a record while another is updating it, the update may "win", causing the record to not be deleted.
* Unique indexes cannot be defined. Similarly, UPSERT (`ON CONFLICT UPDATE`) is not supported.
* Table manipulations (DDL) are not supported.

### References
* [HAWQ-304](https://issues.apache.org/jira/browse/HAWQ-304) Support update and delete on non-heap tables
* [HIVE-5317](https://issues.apache.org/jira/browse/HIVE-5317) Implement insert, update, and delete in Hive with full ACID support
* [Four steps strategy for incremental updates in Hive](https://hortonworks.com/blog/four-step-strategy-incremental-updates-hive/)
