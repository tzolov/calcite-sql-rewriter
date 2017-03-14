DROP SCHEMA IF EXISTS hr CASCADE;
CREATE SCHEMA hr;

CREATE TABLE hr.depts_journal (
  deptno                    SERIAL                   NOT NULL,
  version_number            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  subsequent_version_number TIMESTAMP WITH TIME ZONE NULL     DEFAULT NULL,
  department_name           TEXT                     NOT NULL,
  PRIMARY KEY (deptno, version_number)
);

CREATE TABLE hr.emps_journal (
  empid                     SERIAL                   NOT NULL,
  deptno                    INT                      NOT NULL,
  version_number            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  subsequent_version_number TIMESTAMP WITH TIME ZONE NULL     DEFAULT NULL,
  first_name                TEXT                     NULL     DEFAULT NULL, -- Nullable test column
  last_name                 TEXT                     NOT NULL, -- Non-nullable test column
  PRIMARY KEY (empid, version_number)
);

CREATE VIEW hr.depts AS
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

CREATE VIEW hr.emps AS
  WITH link_last AS (
      SELECT
        *,
        MAX(version_number)
        OVER (PARTITION BY empid) AS last_version_number
      FROM hr.emps_journal
  )
  SELECT
    empid,
    deptno
  FROM link_last
  WHERE subsequent_version_number IS NULL
        AND version_number = last_version_number;

INSERT INTO hr.depts_journal (deptno, department_name) VALUES (1, 'Dep1'), (2, 'Dep2'), (3, 'Dep3'), (4, 'Dep4');
INSERT INTO hr.emps_journal (empid, deptno, first_name, last_name)
VALUES (1, 1, 'Peter', 'Pan'), (2, 1, 'Ian', 'Bibian'), (3, 2, 'Victor', 'Strugatski'), (4, 2, 'Stan', 'Ban'),
  (5, 2, 'Dimitar', 'Gergov'), (6, 4, 'Ivan', 'Baraban');

-- Employee 1 moves to department 2
INSERT INTO hr.emps_journal (empid, deptno, first_name, last_name) VALUES (1, 2, 'Peter', 'Pan');


-- How the overwite rules works

-- INSERT --
-- From:
INSERT INTO hr.depts (deptno, department_name) VALUES(666, 'Pivotal');
-- To:
INSERT INTO hr.depts_journal (deptno, department_name) VALUES (666, 'Pivotal');

-- UPDATE --
-- From:
UPDATE hr.depts SET department_name='New Name' WHERE deptno = 666;
-- To:
INSERT INTO hr.depts_journal (deptno, department_name)
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
    'New Name' as department_name
  FROM link_last
  WHERE subsequent_version_number IS NULL
        AND version_number = last_version_number;

-- DELETE --
-- From:
DELETE FROM hr.depts WHERE deptno=666;
-- To:
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

-- To(2):
INSERT INTO hr.depts_journal (deptno, department_name, version_number, subsequent_version_number)
(
 SELECT
   deptno,
   department_name,
   CURRENT_TIMESTAMP AS version_number,
   CURRENT_TIMESTAMP AS subsequent_version_number
 FROM (
   SELECT
     deptno,
     department_name,
     version_number,
     subsequent_version_number,
     MAX(version_number) OVER (PARTITION BY deptno) AS $f4
   FROM hr.depts_journal) AS t
 WHERE version_number = $f4
       AND subsequent_version_number IS NULL
       AND deptno = 666);
