DROP SCHEMA IF EXISTS ${ACTUAL_SCHEMA} CASCADE;
CREATE SCHEMA ${ACTUAL_SCHEMA};

CREATE TABLE ${ACTUAL_SCHEMA}.depts_journal (
  deptno                    SERIAL          NOT NULL,
  department_name           TEXT            NULL     DEFAULT NULL, -- Nullable test column
  version_number            BIGINT NOT NULL DEFAULT 1,
  subsequent_version_number BIGINT NULL     DEFAULT NULL,
  PRIMARY KEY (deptno, version_number)
);

CREATE TABLE ${ACTUAL_SCHEMA}.emps_journal (
  empid                     SERIAL          NOT NULL,
  deptno                    INT             NOT NULL,
  version_number            BIGINT NOT NULL DEFAULT 1,
  subsequent_version_number BIGINT NULL     DEFAULT NULL,
  first_name                TEXT            NULL     DEFAULT NULL, -- Nullable test column
  last_name                 TEXT            NOT NULL, -- Non-nullable test column
  PRIMARY KEY (empid, version_number)
);

CREATE TABLE ${ACTUAL_SCHEMA}.non_journalled (
  id SERIAL NOT NULL PRIMARY KEY
);

CREATE VIEW ${ACTUAL_SCHEMA}.depts AS
  SELECT * FROM ${ACTUAL_SCHEMA}.emps_journal; -- A view which should be ignored

INSERT INTO ${ACTUAL_SCHEMA}.depts_journal (deptno, department_name) VALUES
  (1, 'Dep1'),
  (2, 'Dep2'),
  (3, 'Dep3'),
  (4, 'Dep4');

INSERT INTO ${ACTUAL_SCHEMA}.emps_journal (empid, deptno, first_name, last_name) VALUES
  (1, 1, 'Peter', 'Pan'),
  (2, 1, 'Ian', 'Bibian'),
  (3, 2, 'Victor', 'Strugatski'),
  (4, 2, 'Stan', 'Ban'),
  (5, 2, 'Dimitar', 'Gergov'),
  (6, 4, 'Ivan', 'Baraban');

-- Employee 1 moves to department 2
INSERT INTO ${ACTUAL_SCHEMA}.emps_journal (empid, deptno, first_name, last_name, version_number)
VALUES (1, 2, 'Peter', 'Pan', 2);

-- Employee 5 left the company
INSERT INTO ${ACTUAL_SCHEMA}.emps_journal (empid, deptno, first_name, last_name, version_number, subsequent_version_number)
VALUES
  (5, 2, 'Dimitar', 'Gergov', 2, 2);

INSERT INTO ${ACTUAL_SCHEMA}.non_journalled (id) VALUES (1), (2);
