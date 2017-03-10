package org.apache.calcite.adapter.jdbc;

import com.google.common.collect.ImmutableList;

import java.util.List;

class JournalledJdbcTable extends JdbcTable {
	private final JdbcTable journalTable;
	private final JournalledJdbcSchema journalledJdbcSchema;
	private final ImmutableList<String> keyColumnNames;

	JournalledJdbcTable(
			String tableName,
			JournalledJdbcSchema journalledJdbcSchema,
			JdbcTable journalTable,
			String[] keyColumnNames
	) {
		super(
				journalledJdbcSchema,
				JdbcTableUtils.getCatalogName(journalTable),
				JdbcTableUtils.getSchemaName(journalTable),
				tableName,
				journalTable.getJdbcTableType()
		);
		this.journalTable = journalTable;
		this.journalledJdbcSchema = journalledJdbcSchema;
		this.keyColumnNames = ImmutableList.copyOf(keyColumnNames);
	}

	JdbcTable getJournalTable() {
		return journalTable;
	}

	List<String> getKeys() {
		return keyColumnNames;
	}

	String getVersionField() {
		return journalledJdbcSchema.getVersionField();
	}

	String getSubsequentVersionField() {
		return journalledJdbcSchema.getSubsequentVersionField();
	}
}
