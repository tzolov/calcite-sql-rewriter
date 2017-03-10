package org.apache.calcite.adapter.jdbc;

import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.io.OutputStream;

class TargetDatabase {
	// Not the nicest citizen in the world; commandeer any standard postgres database we find on the machine.
	// We'll make a schema to hold everything we do, so we won't be too messy overall.

	// Assume standard postgres defaults
	private static final String DB_URL = "postgresql://localhost:5432/postgres";
	private static final String DB_USER = System.getProperty("user.name");
	private static final String DB_PASS = "";

	static final String JOURNALLED_MODEL = "{\n"
			+ "  \"version\": \"1.0\",\n"
			+ "  \"defaultSchema\": \"dontrelyonme\",\n"
			+ "   schemas: [\n"
			+ "     {\n"
			+ "       \"name\": \"hr\",\n"
			+ "       \"type\": \"custom\",\n"
			+ "       \"factory\": \"org.apache.calcite.adapter.jdbc.JournalledJdbcSchema$Factory\",\n"
			+ "       \"operand\": {\n"
			+ "         \"jdbcDriver\": \"org.postgresql.Driver\",\n"
			+ "         \"jdbcUser\": \"" + DB_USER + "\",\n"
			+ "         \"jdbcPassword\": \"" + DB_PASS + "\",\n"
			+ "         \"jdbcUrl\": \"jdbc:" + DB_URL + "\",\n"
			+ "         \"jdbcSchema\": \"calcite_sql_rewriter_integration_test\",\n"
			+ "         \"journalSuffix\": \"_journal\",\n"
			+ "         \"journalVersionField\": \"version_number\",\n"
			+ "         \"journalSubsequentVersionField\": \"subsequent_version_number\",\n"
			+ "         \"journalDefaultKey\": [\"id\"],\n"
			+ "         \"journalTables\": {\n"
			+ "           \"emps\": [\"empid\"],\n"
			+ "           \"depts\": \"deptno\"\n"
			+ "         }\n"
			+ "       }\n"
			+ "     }\n"
			+ "   ]\n"
			+ "}";

	static void rebuild() throws Exception {
		// Splitting commands at semicolons is hard; let's go delegate!
		Process cmd = new ProcessBuilder()
				.command("psql", DB_URL + "?user=" + DB_USER + "&password=" + DB_PASS)
				.redirectOutput(ProcessBuilder.Redirect.INHERIT)
				.redirectError(ProcessBuilder.Redirect.INHERIT)
				.start();
		OutputStream outputStream = cmd.getOutputStream();
		InputStream scriptStream = ClassLoader.getSystemResourceAsStream("TestDB.sql");
		IOUtils.copy(scriptStream, outputStream);
		outputStream.close();
		scriptStream.close();
		cmd.waitFor();
	}
}
