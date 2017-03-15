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
additional metadata columns: `version_number` and `subsequent_version_number` of either `TIMESTAMP` or `BIGINT` type. 

`sql-rewrite` leverages [Apache Calcite](https://calcite.apache.org/) to implement a `JDBC adapter` between the end-users 
and the backend SQL-on-Hadoop system. It exposes a fully-fledged `JDBC` interface to the end-users while internally 
converts the incoming `INSERT`, `UPDATE` and `DELETE` into append-only `INSERTs` and forwards later to 
the backend DB (aka [Apache HAWQ](http://hawq.incubator.apache.org/) ).

### How It Works

Lets have a Department table called `depts`, with `deptno` (key) and `department_name` columns:
```sql
CREATE TABLE hr.depts (
  deptno                    SERIAL                   NOT NULL,
  department_name           TEXT                     NOT NULL,
  PRIMARY KEY (deptno)
);
```
The `sql-rewrite` convention requires you to create a corresponding journal table named  
`<your-table-name>_journal`, with the same schema as the original table plus two metadata columns: `version_number` 
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
* `version_number` - version when the row that was inserted. An increasing number, the highest value represents the current row state. 
* `subsequent_version_number` - marks deleted rows and can be populated on older records by background archival tasks.

Note that the new key is composed of the original key(s) `deptno` and the `version_number`! 

Below are few sample `INSERT`, `UPDATE`, `DELETE` and `SELECT` statements and their internal representation.

1. Issuing an `INSERT` against the Calcite JDBC driver
```sql
INSERT INTO hr.depts (deptno, department_name) VALUES (666, 'Pivotal');
```
is translated into following SQL statement
```sql
INSERT INTO hr.depts_journal (deptno, department_name) VALUES (666, 'Pivotal');
```
Note that the table name is replaced from `depts` to `depts_journal`. Actually the `depts` table may not even exist. 
Data is always stored by the `depts_journal` table!

2. `UPDATE` issued against the Calcite JDBC
```sql
UPDATE hr.depts SET department_name='New Name' WHERE deptno = 666;
```
is expanded into an `INSERT/SELECT` statement like this
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
3. `DELETE` issued against the Calcite JDBC
```sql
DELETE FROM hr.depts WHERE deptno=666;
```
is expanded into an `INSERT/SELECT` statement like this
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

4. `SELECT` query against the Calcite JDBC
```sql
SELECT * FROM hr.depts;
```
is converted into `SELECT` such as
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
For every `deptno` only the row with the highest`version_number` is returned. 

The `MAX(version_number) OVER (PARTITION BY deptno)` [window function](https://www.postgresql.org/docs/9.6/static/tutorial-window.html) 
computes the max `version_number` per `deptno`.

### Configuration

###### Calcite JDBC connection

To connect to the SQL-Rewrite JDBC driver you need to provide [model](https://calcite.apache.org/docs/model.html) JSON 
document. Models can also be built programmatically. Model is comprised of two group of attributes: (1) Calcite generic 
as explained here [model attributes](https://calcite.apache.org/docs/model.html) and (2) `sql-rewrite` specific attributes 
set via the `operand` properties. Table below explains the specific properties.


| Property        | Description       | Default  |
| ------------- |:-------------|:-----|
| connection           | Path to the backend jdbc connection configuration file | none |
| jdbcSchema           |  | none |
| journalSuffix        | Journal table suffix | _journal |
| journalVersionField  | Journal table version number column name | version_number |
| journalSubsequentVersionField | Journal table delete flag column name | subsequent_version_number |
| journalDefaultKey      | Name of the default table column `ID` used when not specified in `journalTables` | id |
| journalTables      | List of journaled tables managed by `sql-rewriter`. Only SQL statements for those tables will be re-written | none |

For example: 

```json
{
  "version": "1.0",
  "defaultSchema": "doesntmatter",
  "schemas": [
    {
      "name": "hr",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.jdbc.JournalledJdbcSchema$Factory",
      "operand": {
        "connection": "myTestConnection.json",
        "jdbcSchema": "hr",
        "journalSuffix": "_journal",
        "journalVersionField": "version_number",
        "journalSubsequentVersionField": "subsequent_version_number",
        "journalDefaultKey": ["id"],
        "journalTables": {
          "emps": ["empid"],
          "depts": ["deptno"]
        }
      }
    }
  ]
}
```

###### Backend DB connection
Backend DB connection configuration is provide in a separate file referred by the `model.json`.
The connection configuration contains the common JDBC connection properties like driver, jdbc ur, 
   and connection credentials.

| Property Name        | Description           | 
| ------------- |:-------------| 
| jdbcDriver      | JDBC driver Class name. For example: `org.postgresql.Driver` | 
| jdbcUrl      | JDBC URL. For example: `jdbc:postgresql://localhost:5432/postgres`    | 
| jdbcUser | The database user on whose behalf the connection is being made. | 
| jdbcPassword | The database user's password.     |  

For example: 

```json
{
  "jdbcDriver": "org.postgresql.Driver",
  "jdbcUrl": "jdbc:postgresql://localhost:5432/postgres",
  "jdbcUser": "pivotal",
  "jdbcPassword": ""
}
```



---
_Note that this project can be used as workaround. Complete solution will be provided with:_ [HAWQ-304](https://issues.apache.org/jira/browse/HAWQ-304).  

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
