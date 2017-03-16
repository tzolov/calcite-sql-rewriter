# SQL Rewriter Spring Boot Sample

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

