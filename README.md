# SQL Rewriter
JDBC driver that converts any `INSERT`, `UPDATE` and `DELETE` statements into append-only `INSERT`s. Instead of 
updating rows in-place it inserts the new version of the row along with version metadata.
[ ![Download](https://api.bintray.com/packages/big-data/maven/calcite-sql-rewriter/images/download.svg) ](https://bintray.com/big-data/maven/calcite-sql-rewriter/_latestVersion)

`SQL on Hadoop` data management systems such as [Apache HAWQ](http://hawq.incubator.apache.org/) do not offer the 
same style of INSERT, UPDATE, and DELETE that users expect of traditional RDBMS. Unlike transactional 
systems, big-data analytical queries are dominated by SELECT over millions or billions of rows. Analytical 
databases are optimized for this kind of workload. The storage systems are optimized for high throughput scans, and 
commonly implemented as immutable (`append-only`) persistence stores. No in-place updates are allowed. 

The `SQL on Hadoop` systems naturally support append-only operations such as `INSERT`. The `UPDATE` or `DELETE` demand 
an alternative approach:
[HAWQ-304](https://issues.apache.org/jira/browse/HAWQ-304), [HIVE-5317](https://issues.apache.org/jira/browse/HIVE-5317)

The `sql-rewrite` project _emulates_ `INSERT`, `UPDATE` and `DELETE` by turning them into
append-only `INSERTs`. Instead of updating rows in-place it inserts the new version of the row using two
additional metadata columns: `version_number` and `subsequent_version_number`. 

* `version_number` holds the version when the row that was inserted in the table. It is an increasing number and 
the highest value represents the current/latest row version. 
* `subsequent_version_number` is used to mark deleted rows, set when the row is marked as deleted, and can be populated 
on older records by background archival tasks.

The `version_number` and `subsequent_version_number` can be of either `TIMESTAMP` or `BIGINT` type.

`sql-rewrite` leverages [Apache Calcite](https://calcite.apache.org/) to implement a `JDBC adapter` between the end-users 
and the backend SQL-on-Hadoop system. It exposes a fully fledged `JDBC` interface to the end-users while internally  
converts the incoming `INSERT`, `UPDATE` and `DELETE` into append-only `INSERTs` and forwards them to 
the backend DB (e.g [Apache HAWQ](http://hawq.incubator.apache.org/) alike).

### How to use

For example lets have a Department table called `depts`, with `deptno` (key) and `department_name` columns:
```sql
CREATE TABLE hr.depts (
  deptno                    SERIAL                   NOT NULL,
  department_name           TEXT                     NOT NULL,
  PRIMARY KEY (deptno)
);
```
The `sql-rewrite` convention requires you to create a corresponding journal table with name: 
`<your-table-name>_journal`), same schema as the original table plus two metadata columns: `version_number` 
and `subsequent_version_number` of `TIMESTAMP` or `BIGINT` type.  The column order does not matter.
```sql
CREATE TABLE hr.depts_journal (
  deptno                    SERIAL                   NOT NULL,
  version_number            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  subsequent_version_number TIMESTAMP WITH TIME ZONE NULL     DEFAULT NULL,
  department_name           TEXT                     NOT NULL,
  PRIMARY KEY (deptno, version_number)
);
```
The `version_number` values are generated on row insert. Latest `version_number` represents the current row state.
The `subsequent_version_number` is set if the row is mark as deleted or stays NULL otherwise.

Note that the new key is composed of the original key(s) `deptno` and the `version_number`! 

Find below how some sample `INSERT`, `UPDATE`, `DELETE` and `SELECT` statements are represent and managed internally.

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
_Note that this project can be used a temporal workaround. Complete solution will be provided with:_ 
[HAWQ-304](https://issues.apache.org/jira/browse/HAWQ-304). _Check the limitations explained below!_ 
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
* [Timestamp-based concurrency control](https://en.wikipedia.org/wiki/Timestamp-based_concurrency_control)
