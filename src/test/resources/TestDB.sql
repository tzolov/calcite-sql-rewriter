DROP SCHEMA IF EXISTS calcite_sql_rewriter_integration_test CASCADE;
CREATE SCHEMA calcite_sql_rewriter_integration_test;

CREATE TABLE calcite_sql_rewriter_integration_test.depts_journal (
  deptno SERIAL NOT NULL,
  version_number TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  subsequent_version_number TIMESTAMP WITH TIME ZONE NULL DEFAULT NULL,
  department_name TEXT NOT NULL,
  PRIMARY KEY (deptno, version_number)
);

CREATE TABLE calcite_sql_rewriter_integration_test.emps_journal (
  empid SERIAL NOT NULL,
  deptno INT NOT NULL,
  version_number TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  subsequent_version_number TIMESTAMP WITH TIME ZONE NULL DEFAULT NULL,
  first_name TEXT NULL DEFAULT NULL, -- Nullable test column
  last_name TEXT NOT NULL, -- Non-nullable test column
  PRIMARY KEY (empid, version_number)
);

CREATE VIEW calcite_sql_rewriter_integration_test.depts AS
  WITH link_last AS (
      SELECT *, MAX(version_number) OVER (PARTITION BY deptno) AS last_version_number
      FROM calcite_sql_rewriter_integration_test.depts_journal
  )
  SELECT deptno, department_name
  FROM link_last
  WHERE subsequent_version_number IS NULL
        AND version_number = last_version_number;

CREATE VIEW calcite_sql_rewriter_integration_test.emps AS
  WITH link_last AS (
      SELECT *, MAX(version_number) OVER (PARTITION BY empid) AS last_version_number
      FROM calcite_sql_rewriter_integration_test.emps_journal
  )
  SELECT empid, deptno
  FROM link_last
  WHERE subsequent_version_number IS NULL
        AND version_number = last_version_number;

INSERT INTO calcite_sql_rewriter_integration_test.depts_journal (deptno, department_name) VALUES (1, 'Dep1'),(2, 'Dep2'),(3, 'Dep3'),(4, 'Dep4');
INSERT INTO calcite_sql_rewriter_integration_test.emps_journal (empid,deptno,first_name,last_name) VALUES (1,1,'Peter','Pan'),(2,1,'Ian','Bibian'),(3,2,'Victor','Strugatski'),(4,2,'Stan','Ban'),(5,2,'Dimitar','Gergov'),(6,4,'Ivan','Baraban');

-- Employee 1 moves to department 2
INSERT INTO calcite_sql_rewriter_integration_test.emps_journal (empid,deptno,first_name,last_name) VALUES (1,2,'Peter','Pan');
