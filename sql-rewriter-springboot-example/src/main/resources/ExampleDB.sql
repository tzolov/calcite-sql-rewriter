DROP SCHEMA IF EXISTS hr CASCADE;
CREATE SCHEMA hr;

-- Create deptas_journal table to represent the depts (virtual table).
CREATE TABLE hr.depts_journal (
  deptno                    SERIAL       NOT NULL,
  department_name           TEXT         NULL      DEFAULT NULL, -- Nullable test column
  version_number            SERIAL       NOT NULL,
  subsequent_version_number BIGINT       NULL      DEFAULT NULL,
  PRIMARY KEY (deptno, version_number)
);

-- Optional View you can create to query the depts table directly from the backend DB (e.g. HAWQ).
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