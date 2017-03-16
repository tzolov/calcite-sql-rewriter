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
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootApplication
public class Application {

	private static final Logger log = LoggerFactory.getLogger(Application.class);

	private JdbcTemplate jdbcTemplate;

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Autowired
	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	@Bean
	public CommandLineRunner demo() {
		return (args) -> {
			log.info("Creating tables");

			// INSERT
			LongStream.range(666, 676).forEach(
					id -> jdbcTemplate.execute("INSERT INTO hr.depts (deptno, department_name) VALUES (" + id + ", 'Dep" + id + "')")
			);

			jdbcTemplate.query(
					"SELECT * FROM hr.depts WHERE deptno > 668",
					(rs, rowNum) -> new Department(rs.getLong("deptno"), rs.getString("department_name"))
			).forEach(department -> log.info(department.toString()));

			// UPDATE
			IntStream.range(666, 676).forEach(
					id -> jdbcTemplate.execute("UPDATE hr.depts SET department_name = 'Boza' WHERE deptno = " + id)
			);

			jdbcTemplate.query(
					"SELECT * FROM hr.depts",
					(rs, rowNum) -> new Department(rs.getLong("deptno"), rs.getString("department_name"))
			).forEach(department -> log.info(department.toString()));

			// DELETE
			IntStream.range(666, 676).forEach(
					id -> jdbcTemplate.execute("DELETE FROM hr.depts WHERE deptno = " + id)
			);

			jdbcTemplate.query(
					"SELECT * FROM hr.depts",
					(rs, rowNum) -> new Department(rs.getLong("deptno"), rs.getString("department_name"))
			).forEach(department -> log.info(department.toString()));


			log.info("");
		};
	}

	@Bean
	@ConfigurationProperties("app.datasource")
	public DataSource dataSource() {
		return DataSourceBuilder.create().build();
	}


//	@Bean
//	public DataSource dataSource() {
//		return DataSourceBuilder
//				.create()
//				.username("admin")
//				.password("admin")
//				.url("jdbc:calcite:model=sql-rewriter-springboot-example/src/main/resources/myTestModel.json;lex=JAVA")
//				.driverClassName("org.apache.calcite.jdbc.Driver")
//				.build();
//	}

}
