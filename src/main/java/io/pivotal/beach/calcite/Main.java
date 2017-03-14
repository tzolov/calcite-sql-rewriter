package io.pivotal.beach.calcite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class Main {
	public static void main(String[] argv) throws Exception {
		String modelPath = (argv.length > 0) ? argv[0] : "src/main/resources/myTestModel2.json";

		Class.forName(org.apache.calcite.jdbc.Driver.class.getName());
		Properties info = new Properties();
		info.setProperty("lex", "JAVA");
		info.setProperty("model", modelPath);
		Connection calConnection = DriverManager.getConnection("jdbc:calcite:", info);

		Statement statement = calConnection.createStatement();
//		String sql = "SELECT d.deptno\n"
//				+ "FROM hr.emps AS e\n"
//				+ "JOIN hr.depts AS d\n"
//				+ "  ON e.deptno = d.deptno\n"
//				+ "GROUP BY d.deptno\n"
//				+ "HAVING count(*) > 1";
		String sql = "INSERT INTO hr.depts (deptno, department_name) VALUES(696, 'Pivotal')";
//		String sql = "INSERT INTO hr.depts_journal\n " +
//				"(deptno, version_number, subsequent_version_number, department_name)\n " +
//				"VALUES(666, TIMESTAMP '2017-03-09 08:07:22', NULL, 'Pivotal')";

		if (statement.execute(sql)) {
			ResultSet results = statement.getResultSet();
			while (results.next()) {
				System.out.println(results.getInt(1));
			}
			results.close();
		} else {
			System.out.printf("Update count: " + statement.getUpdateCount());
		}

		statement.close();
		calConnection.close();
	}
}
