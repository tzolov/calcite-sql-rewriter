### Calcite JDBC connection

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

### Backend DB connection
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
