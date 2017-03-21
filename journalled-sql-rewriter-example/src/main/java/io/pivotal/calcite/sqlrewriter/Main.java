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
package io.pivotal.calcite.sqlrewriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

	private static final Logger log = LoggerFactory.getLogger(Main.class);

	public static void main(String[] argv) throws Exception {

		String modelPath = argv.length > 0? argv[0] : "journalled-sql-rewriter-example/src/main/resources/myTestModel.json";

		Class.forName(org.apache.calcite.jdbc.Driver.class.getName());
		Properties info = new Properties();

		info.setProperty("lex", "JAVA");
		info.setProperty("model", modelPath);
		Connection calConnection = DriverManager.getConnection("jdbc:calcite:", info);

		Statement statement = calConnection.createStatement();

		runSql("INSERT INTO hr.depts (deptno, department_name) VALUES(696, 'Pivotal')", statement);

		runSql("SELECT * FROM hr.depts", statement);

		runSql("UPDATE hr.depts SET department_name='interma' WHERE deptno = 696", statement);

		runSql("SELECT * FROM hr.depts", statement);

		runSql("DELETE FROM hr.depts WHERE deptno = 696", statement);

		runSql("SELECT * FROM hr.depts", statement);

		log.info("Done");

		statement.close();
		calConnection.close();
	}

	private static void runSql(String sql, Statement statement) throws SQLException {
		log.info(sql);
		if (statement.execute(sql)) {
			printResultSet(statement.getResultSet());
		} else {
			log.info("  updated rows: " + statement.getUpdateCount());
		}
	}

	private static void printResultSet(ResultSet resultSet) throws SQLException {
		int columnCount = resultSet.getMetaData().getColumnCount();
		String rowString = "";
		while (resultSet.next()) {
			for (int i = 1; i <= columnCount; i++) {
				rowString = rowString + resultSet.getString(i) + " , ";
			}
		}
		log.info("  result: " + rowString);
		resultSet.close();
	}
}
