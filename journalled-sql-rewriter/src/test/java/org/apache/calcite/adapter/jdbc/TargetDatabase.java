package org.apache.calcite.adapter.jdbc;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;

class TargetDatabase {
	// Not the nicest citizen in the world; commandeer any standard postgres database we find on the machine.
	// We'll make a schema to hold everything we do, so we won't be too messy overall.

	// Assume standard postgres defaults
	private static final String DB_URL = "postgresql://localhost:5432/postgres";
	private static final String DB_USER = System.getProperty("user.name");
	private static final String DB_PASS = "";

	static final String JOURNALLED_MODEL_TEMPLATE = "{\n"
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
			+ "         \"journalVersionType\": \"{VERSION_TYPE}\",\n"
			+ "         \"journalDefaultKey\": [\"id\"],\n"
			+ "         \"journalTables\": {\n"
			+ "           \"emps\": [\"empid\"],\n"
			+ "           \"depts\": \"deptno\"\n"
			+ "         }\n"
			+ "       }\n"
			+ "     },\n"
			+ "     {\n" // This exists to work around a bug in Calcite [https://issues.apache.org/jira/browse/CALCITE-1692]. Once the bug is fixed, the tests using this schema should switch to use hr instead.
			+ "       \"name\": \"calcite_sql_rewriter_integration_test\",\n"
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
			+ "         \"journalVersionType\": \"{VERSION_TYPE}\",\n"
			+ "         \"journalDefaultKey\": [\"id\"],\n"
			+ "         \"journalTables\": {\n"
			+ "           \"emps\": [\"empid\"],\n"
			+ "           \"depts\": \"deptno\"\n"
			+ "         }\n"
			+ "       }\n"
			+ "     }\n"
			+ "   ]\n"
			+ "}";

	static String makeJournalledModel(JournalledJdbcSchema.VersionType versionType) {
		return JOURNALLED_MODEL_TEMPLATE.replace("{VERSION_TYPE}", versionType.name());
	}

	static void rebuild(JournalledJdbcSchema.VersionType type) throws Exception {
		// Splitting commands at semicolons is hard; let's go delegate!
		Process cmd = new ProcessBuilder()
				.command("psql", DB_URL + "?user=" + DB_USER + "&password=" + DB_PASS)
				.redirectOutput(ProcessBuilder.Redirect.INHERIT)
				.redirectError(ProcessBuilder.Redirect.INHERIT)
				.start();
		OutputStream outputStream = cmd.getOutputStream();
		String resource = null;
		switch(type) {
			case TIMESTAMP:
				resource = "TimestampVersionDB.sql";
				break;
			case BIGINT:
				resource = "BigintVersionDB.sql";
				break;
		}
		InputStream scriptStream = ClassLoader.getSystemResourceAsStream(resource);
		IOUtils.copy(scriptStream, outputStream);
		outputStream.close();
		scriptStream.close();
		int exitCode = cmd.waitFor();
		if(exitCode != 0) {
			throw new IllegalStateException("Failed to build test database. Exit code: " + exitCode);
		}
	}
}
