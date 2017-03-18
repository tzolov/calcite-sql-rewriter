package io.pivotal.calcite.example;

import java.util.stream.LongStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootApplication
public class Application implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(Application.class);

	@Autowired
	@Qualifier("calciteJdbcTemplate")
	private JdbcTemplate calciteJdbcTemplate;

	@Autowired
	@Qualifier("postgresJdbcTemplate")
	private JdbcTemplate postgresJdbcTemplate;

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Override
	public void run(String... strings) throws Exception {

		log.info("--- CREATE depts_journal table --- ");

		postgresJdbcTemplate.execute("DROP SCHEMA IF EXISTS hr CASCADE");
		postgresJdbcTemplate.execute("CREATE SCHEMA hr");

		postgresJdbcTemplate.execute("CREATE TABLE hr.depts_journal (\n" +
				"  deptno                    SERIAL                   NOT NULL,\n" +
				"  department_name           TEXT                     NULL     DEFAULT NULL, -- Nullable test column\n" +
				"  version_number            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
				"  subsequent_version_number TIMESTAMP WITH TIME ZONE NULL     DEFAULT NULL,\n" +
				"  PRIMARY KEY (deptno, version_number)\n" +
				")");

		log.info("--- INSERT in depts table --- ");

		LongStream.range(666, 669).forEach(
				id -> calciteJdbcTemplate.execute("INSERT INTO hr.depts (deptno, department_name) VALUES ("
						+ id + ", 'Department" + id + "')")
		);

		queryAllDepts();

		log.info("--- UPDATE depts table --- ");

		LongStream.range(666, 669).forEach(
				id -> calciteJdbcTemplate.execute("UPDATE hr.depts SET department_name = 'NewName' WHERE deptno = " + id)
		);

		queryAllDepts();

		log.info("--- DELETE depts table rows --- ");

		LongStream.range(666, 669).forEach(
				id -> calciteJdbcTemplate.execute("DELETE FROM hr.depts WHERE deptno = " + id)
		);

		queryAllDepts();

		log.info("--- INSERT in depts table --- ");

		LongStream.range(666, 669).forEach(
				id -> calciteJdbcTemplate.execute("INSERT INTO hr.depts (deptno, department_name) VALUES ("
						+ id + ", 'Department" + id + "')")
		);
		queryAllDepts();

		log.info("--- QUERY all depts_journal --- ");
		postgresJdbcTemplate.query(
				"SELECT * FROM hr.depts_journal",
				(rs, rowNum) -> "ver: [" + rs.getString("version_number") + "] "
						+ "sub_ver: [" + rs.getString("subsequent_version_number") + "] "
						+ "deptsno: [" + rs.getLong("deptno") + "] "
						+ "depts_name: [" + rs.getString("department_name") + "]"
		).forEach(department_journal -> log.info("      " + department_journal.toString()));

		log.info("--- END --- ");
	}

	public void queryAllDepts() {
		log.info("--- QUERY all depts --- ");
		calciteJdbcTemplate.query(
				"SELECT * FROM hr.depts",
				(rs, rowNum) -> new Department(rs.getLong("deptno"), rs.getString("department_name"))
		).forEach(department -> log.info("      " + department.toString()));
	}
}
