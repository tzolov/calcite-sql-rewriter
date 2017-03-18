/*
 * Copyright 2012-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

		log.info("1. CREATE depts_journal table");

		postgresJdbcTemplate.execute("DROP SCHEMA IF EXISTS hr CASCADE");
		postgresJdbcTemplate.execute("CREATE SCHEMA hr");

		postgresJdbcTemplate.execute("CREATE TABLE hr.depts_journal (\n" +
				"  deptno                    SERIAL          NOT NULL,\n" +
				"  department_name           TEXT            NULL     DEFAULT NULL, -- Nullable test column\n" +
				"  version_number            SERIAL          NOT NULL ,\n" +
				"  subsequent_version_number BIGINT NULL     DEFAULT NULL,\n" +
				"  PRIMARY KEY (deptno, version_number)\n" +
				")");

		queryAllDepts();
		queryAllDeptsJournal();

		log.info("2. INSERT 3 rows in depts");

		LongStream.range(666, 669).forEach(
				id -> calciteJdbcTemplate.execute("INSERT INTO hr.depts (deptno, department_name) VALUES ("
						+ id + ", 'Department" + id + "')")
		);

		queryAllDepts();
		queryAllDeptsJournal();

		log.info("3. UPDATE all depts rows");

		LongStream.range(666, 669).forEach(
				id -> calciteJdbcTemplate.execute("UPDATE hr.depts SET department_name = 'NewName' WHERE deptno = " + id)
		);

		queryAllDepts();
		queryAllDeptsJournal();

		log.info("4. DELETE all depts rows");

		LongStream.range(666, 669).forEach(
				id -> calciteJdbcTemplate.execute("DELETE FROM hr.depts WHERE deptno = " + id)
		);

		queryAllDepts();
		queryAllDeptsJournal();

		log.info("5. INSERT 3 rows in depts again");

		LongStream.range(666, 669).forEach(
				id -> calciteJdbcTemplate.execute("INSERT INTO hr.depts (deptno, department_name) VALUES ("
						+ id + ", 'Second Insert " + id + "')")
		);

		queryAllDepts();
		queryAllDeptsJournal();

		log.info("6. END");
	}

	private void queryAllDepts() {
		log.info("     QUERY all depts");
		long count = calciteJdbcTemplate.query(
				"SELECT * FROM hr.depts",
				(rs, rowNum) -> new Department(rs.getLong("deptno"), rs.getString("department_name"))
		).stream().map(department -> {
			log.info("       " + department.toString());
			return department.toString();
		})
				.count();
		log.info("       Count:" + count);
	}

	private void queryAllDeptsJournal() {
		log.info("     QUERY all depts_journal");
		long count = postgresJdbcTemplate.query(
				"SELECT * FROM hr.depts_journal",
				(rs, rowNum) -> "ver: [" + rs.getString("version_number") + "] "
						+ "sub_ver: [" + rs.getString("subsequent_version_number") + "] "
						+ "deptsno: [" + rs.getLong("deptno") + "] "
						+ "depts_name: [" + rs.getString("department_name") + "]"
		).stream().map(department_journal -> {
			log.info("       " + department_journal.toString());
			return department_journal.toString();
		}).count();
		log.info("       Count:" + count);
	}
}
