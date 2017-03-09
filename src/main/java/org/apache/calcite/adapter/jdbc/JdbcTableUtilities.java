package org.apache.calcite.adapter.jdbc;

import io.pivotal.beach.calcite.configuration.JournalledTableConfiguration;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlIdentifier;

import java.lang.reflect.Field;

public class JdbcTableUtilities {
	public static Schema getSchema(JdbcTable table) {
		try {
			Field schemaF = JdbcTable.class.getDeclaredField("jdbcSchema");
			schemaF.setAccessible(true);
			return (Schema) schemaF.get(table);
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	public static String getName(JdbcTable table) {
		// tableName is: [catalog,] [schema,] table
		SqlIdentifier identifier = table.tableName();
		return identifier.names.get(identifier.names.size() - 1);
	}

	public static JournalledTableConfiguration configurationForTable(JdbcTable jdbcTable) {
		return JournalledTableConfiguration.get(
				JdbcTableUtilities.getSchema(jdbcTable),
				JdbcTableUtilities.getName(jdbcTable)
		);
	}
}
