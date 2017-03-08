package io.pivotal.beach.calcite;

import io.pivotal.beach.calcite.programs.ForcedRulesProgram;
import io.pivotal.beach.calcite.programs.SequenceProgram;
import org.apache.calcite.adapter.jdbc.JournalledSelectRule;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.runtime.Hook;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class Main {
	public static void main(String[] argv) throws Exception {
		Hook.PROGRAM.add(SequenceProgram.prepend(new ForcedRulesProgram(
				new JournalledSelectRule()
		)));
		Hook.QUERY_PLAN.add((String o) -> {
			System.out.println(o);
			return null;
		});

		String modelPath = argv.length > 0? argv[0]:"src/main/resources/myTestModel2.json";

		Class.forName(org.apache.calcite.jdbc.Driver.class.getName());
		Properties info = new Properties();
		info.setProperty("lex", "JAVA");
		info.setProperty("model", modelPath);
		info.setProperty("test1", "Hi!");
		Connection calConnection = DriverManager.getConnection("jdbc:calcite:", info);
		CalciteConnection calciteConnection = calConnection.unwrap(CalciteConnection.class);

		Statement statement = calciteConnection.createStatement();
		String sql = "SELECT d.deptno\n"
				+ "FROM hr.emps AS e\n"
				+ "JOIN hr.depts AS d\n"
				+ "  ON e.deptno = d.deptno\n"
				+ "GROUP BY d.deptno\n"
				+ "HAVING count(*) > 1";

		if (statement.execute(sql)) {
			ResultSet results =statement.getResultSet();
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
