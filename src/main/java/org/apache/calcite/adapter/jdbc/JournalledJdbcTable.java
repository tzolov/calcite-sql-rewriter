package org.apache.calcite.adapter.jdbc;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class JournalledJdbcTable extends JdbcTable {
	private final JdbcTable journalTable;
	private final JournalledJdbcSchema journalledJdbcSchema;
	private final ImmutableList<String> keyColumnNames;

	public JournalledJdbcTable(
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

	public JdbcTable getJournalTable() {
		return journalTable;
	}

	public List<String> getKeys() {
		return keyColumnNames;
	}

	public String getVersionField() {
		return journalledJdbcSchema.getVersionField();
	}

	public String getSubsequentVersionField() {
		return journalledJdbcSchema.getSubsequentVersionField();
	}
}
