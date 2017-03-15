package org.apache.calcite.adapter.jdbc;

import javax.sql.DataSource;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;

class TargetDatabase {
	// Not the nicest citizen in the world; commandeer any standard postgres database we find on the machine.
	// We'll make a schema to hold everything we do, so we won't be too messy overall.

	// Assume standard postgres defaults
	private static final String DB_DRIVER = "org.postgresql.Driver";
	private static final String DB_URL = "postgresql://localhost:5432/postgres";
	private static final String DB_USER = System.getProperty("user.name");
	private static final String DB_PASS = "";

	static {
		// Run before any tests, but only need to run once

		try {
			for(JournalVersionType versionType : JournalVersionType.values()) {
				rebuild(versionType);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unused") // Used by the model JSON
	public static class IsolatedDataSource implements DataSource {
		public static final DataSource INSTANCE = new IsolatedDataSource();
		private final DataSource base;

		private IsolatedDataSource() {
			base = JdbcSchema.dataSource("jdbc:" + DB_URL, DB_DRIVER, DB_USER, DB_PASS);
		}

		@Override
		public Connection getConnection() throws SQLException {
			Connection c = base.getConnection();
			c.setAutoCommit(false);
			return c;
		}

		@Override
		public Connection getConnection(String username, String password) throws SQLException {
			Connection c = base.getConnection(username, password);
			c.setAutoCommit(false);
			return c;
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			return base.unwrap(iface);
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			return base.isWrapperFor(iface);
		}

		@Override
		public PrintWriter getLogWriter() throws SQLException {
			return base.getLogWriter();
		}

		@Override
		public void setLogWriter(PrintWriter out) throws SQLException {
			base.setLogWriter(out);
		}

		@Override
		public void setLoginTimeout(int seconds) throws SQLException {
			base.setLoginTimeout(seconds);
		}

		@Override
		public int getLoginTimeout() throws SQLException {
			return base.getLoginTimeout();
		}

		@Override
		public Logger getParentLogger() throws SQLFeatureNotSupportedException {
			return base.getParentLogger();
		}
	}

	private static final String JOURNALLED_MODEL_TEMPLATE = "{\n"
			+ "  \"version\": \"1.0\",\n"
			+ "  \"defaultSchema\": \"dontrelyonme\",\n"
			+ "   schemas: [\n"
			+ "     {\n"
			+ "       \"name\": \"${VIRTUAL_SCHEMA}\",\n"
			+ "       \"type\": \"custom\",\n"
			+ "       \"factory\": \"org.apache.calcite.adapter.jdbc.JournalledJdbcSchema$Factory\",\n"
			+ "       \"operand\": {\n"
			+ "         \"dataSource\": \"org.apache.calcite.adapter.jdbc.TargetDatabase$IsolatedDataSource\",\n"
			+ "         \"jdbcSchema\": \"${ACTUAL_SCHEMA}\",\n"
			+ "         \"journalSuffix\": \"_journal\",\n"
			+ "         \"journalVersionField\": \"version_number\",\n"
			+ "         \"journalSubsequentVersionField\": \"subsequent_version_number\",\n"
			+ "         \"journalVersionType\": \"${VERSION_TYPE}\",\n"
			+ "         \"journalDefaultKey\": [\"id\"],\n"
			+ "         \"journalTables\": {\n"
			+ "           \"emps\": [\"empid\"],\n"
			+ "           \"depts\": \"deptno\"\n"
			+ "         }\n"
			+ "       }\n"
			+ "     },\n"
			+ "     {\n" // TODO: This exists to work around a bug in Calcite [https://issues.apache.org/jira/browse/CALCITE-1692]. Once the bug is fixed, the tests using this schema should switch to use hr instead.
			+ "       \"name\": \"${ACTUAL_SCHEMA}\",\n"
			+ "       \"type\": \"custom\",\n"
			+ "       \"factory\": \"org.apache.calcite.adapter.jdbc.JournalledJdbcSchema$Factory\",\n"
			+ "       \"operand\": {\n"
			+ "         \"dataSource\": \"org.apache.calcite.adapter.jdbc.TargetDatabase$IsolatedDataSource\",\n"
			+ "         \"jdbcSchema\": \"${ACTUAL_SCHEMA}\",\n"
			+ "         \"journalSuffix\": \"_journal\",\n"
			+ "         \"journalVersionField\": \"version_number\",\n"
			+ "         \"journalSubsequentVersionField\": \"subsequent_version_number\",\n"
			+ "         \"journalVersionType\": \"${VERSION_TYPE}\",\n"
			+ "         \"journalDefaultKey\": [\"id\"],\n"
			+ "         \"journalTables\": {\n"
			+ "           \"emps\": [\"empid\"],\n"
			+ "           \"depts\": \"deptno\"\n"
			+ "         }\n"
			+ "       }\n"
			+ "     }\n"
			+ "   ]\n"
			+ "}";

	static String getVirtualSchema(JournalVersionType versionType) {
		return "hr_" + versionType.toString().toLowerCase();
	}

	static String getActualSchema(JournalVersionType versionType) {
		return "calcite_sql_rewriter_integration_test_" + versionType.toString().toLowerCase();
	}

	static String makeJournalledModel(JournalVersionType versionType) {
		return substitute(JOURNALLED_MODEL_TEMPLATE, versionType);
	}

	private static void rebuild(JournalVersionType versionType) throws Exception {
		// Splitting commands at semicolons is hard; let's go delegate!
		Process cmd = new ProcessBuilder()
				.command("psql", DB_URL + "?user=" + DB_USER + "&password=" + DB_PASS)
				.redirectOutput(ProcessBuilder.Redirect.INHERIT)
				.redirectError(ProcessBuilder.Redirect.INHERIT)
				.start();
		OutputStream outputStream = cmd.getOutputStream();
		String resource = null;
		switch(versionType) {
			case TIMESTAMP:
				resource = "TimestampVersionDB.sql";
				break;
			case BIGINT:
				resource = "BigintVersionDB.sql";
				break;
		}
		InputStream scriptStream = ClassLoader.getSystemResourceAsStream(resource);
		InputStream convertedSql = IOUtils.toInputStream(substitute(IOUtils.toString(scriptStream), versionType));
		IOUtils.copy(convertedSql, outputStream);
		scriptStream.close();
		convertedSql.close();
		outputStream.close();
		int exitCode = cmd.waitFor();
		if(exitCode != 0) {
			throw new IllegalStateException("Failed to build test database. Exit code: " + exitCode);
		}
	}

	private static String substitute(String sql, JournalVersionType versionType) {
		return sql
				.replace("${VERSION_TYPE}", versionType.name())
				.replace("${VIRTUAL_SCHEMA}", getVirtualSchema(versionType))
				.replace("${ACTUAL_SCHEMA}", getActualSchema(versionType));
	}
}
