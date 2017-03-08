DROP SCHEMA IF EXISTS hr CASCADE;
CREATE SCHEMA hr;

CREATE TABLE hr.depts_journal (
  deptno SERIAL NOT NULL,
  version_number TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  subsequent_version_number TIMESTAMP WITH TIME ZONE NULL DEFAULT NULL,
  PRIMARY KEY (deptno, version_number)
);

CREATE TABLE hr.emps_journal (
  empid SERIAL NOT NULL,
  deptno INT NOT NULL,
  version_number TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  subsequent_version_number TIMESTAMP WITH TIME ZONE NULL DEFAULT NULL,
  PRIMARY KEY (empid, version_number)
);

CREATE VIEW hr.depts AS
  WITH link_last AS (
      SELECT *, MAX(version_number) OVER (PARTITION BY deptno) AS last_version_number
      FROM hr.depts_journal
  )
  SELECT deptno
  FROM link_last
  WHERE subsequent_version_number IS NULL
        AND version_number = last_version_number;

CREATE VIEW hr.emps AS
  WITH link_last AS (
      SELECT *, MAX(version_number) OVER (PARTITION BY empid) AS last_version_number
      FROM hr.emps_journal
  )
  SELECT empid, deptno
  FROM link_last
  WHERE subsequent_version_number IS NULL
        AND version_number = last_version_number;

INSERT INTO hr.depts_journal (deptno) VALUES (1),(2),(3),(4);
INSERT INTO hr.emps_journal (empid,deptno) VALUES (1,1),(2,1),(3,2),(4,2),(5,2),(6,4);

-- Employee 1 moves to department 2
INSERT INTO hr.emps_journal (empid,deptno) VALUES (1,2);
