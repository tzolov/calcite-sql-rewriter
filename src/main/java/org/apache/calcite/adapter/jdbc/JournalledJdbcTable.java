package org.apache.calcite.adapter.jdbc;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexNode;

import java.util.Collections;
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

	@Override public TableModify toModificationRel(
			RelOptCluster cluster,
			RelOptTable table,
			Prepare.CatalogReader catalogReader,
			RelNode input,
			TableModify.Operation operation,
			List<String> updateColumnList,
			List<RexNode> sourceExpressionList,
			boolean flattened
	) {
		List<String> names = JdbcTableUtils.getQualifiedName(table, journalTable);
		RelOptTable relOptJournalTable = table.getRelOptSchema().getTableForMember(names);
		return journalTable.toModificationRel(
				cluster,
				relOptJournalTable,
				catalogReader,
				input,
				operation,
				updateColumnList,
				sourceExpressionList,
				flattened
		);
	}
}
