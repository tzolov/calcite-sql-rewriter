package io.pivotal.calcite.example;

import java.util.stream.IntStream;
import java.util.stream.LongStream;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootApplication
public class Application implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(Application.class);

	private JdbcTemplate jdbcTemplate;

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Autowired
	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	@Override
	public void run(String... strings) throws Exception {
		log.info("Insert ROWs");

		LongStream.range(666, 676).forEach(
				id -> jdbcTemplate.execute("INSERT INTO hr.depts (deptno, department_name) VALUES (" + id + ", 'Dep" + id + "')")
		);

		jdbcTemplate.query(
				"SELECT * FROM hr.depts WHERE deptno > 668",
				(rs, rowNum) -> new Department(rs.getLong("deptno"), rs.getString("department_name"))
		).forEach(department -> log.info(department.toString()));

		log.info("UPDATE ROWs");
		IntStream.range(666, 676).forEach(
				id -> jdbcTemplate.execute("UPDATE hr.depts SET department_name = 'Boza' WHERE deptno = " + id)
		);

		jdbcTemplate.query(
				"SELECT * FROM hr.depts",
				(rs, rowNum) -> new Department(rs.getLong("deptno"), rs.getString("department_name"))
		).forEach(department -> log.info(department.toString()));

		log.info("DELETE ROWs");
		IntStream.range(666, 676).forEach(
				id -> jdbcTemplate.execute("DELETE FROM hr.depts WHERE deptno = " + id)
		);

		jdbcTemplate.query(
				"SELECT * FROM hr.depts",
				(rs, rowNum) -> new Department(rs.getLong("deptno"), rs.getString("department_name"))
		).forEach(department -> log.info(department.toString()));
	}
}
