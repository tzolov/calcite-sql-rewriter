package org.apache.calcite.adapter.jdbc;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import io.pivotal.beach.calcite.programs.BasicForcedRule;

public class JournalledDeleteRule implements BasicForcedRule {
	@Override
	public RelNode apply(RelNode originalRel, JdbcRelBuilderFactory relBuilderFactory) {
		if (!(originalRel instanceof LogicalTableModify)) {
			return null;
		}

		LogicalTableModify tableModify = (LogicalTableModify) originalRel;

		if (!tableModify.isDelete()) {
			return null;
		}

		Table baseTable = JdbcTableUtils.getJdbcTable(tableModify);
		if(!(baseTable instanceof JournalledJdbcTable)) {
			// Not a journal table; nothing to do
			return null;
		}
		JournalledJdbcTable journalTable = (JournalledJdbcTable) baseTable;

		JdbcRelBuilder relBuilder = relBuilderFactory.create(
				originalRel.getCluster(),
				tableModify.getTable().getRelOptSchema()
		);
		relBuilder.push(tableModify.getInput());

		List<String> columnNames = new ArrayList<>();
		List<RexNode> sources = new ArrayList<>();
		for(RexNode n : relBuilder.fields()) {
			sources.add(n);
			columnNames.add(null);
		}

		sources.add(relBuilder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP));
		sources.add(relBuilder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP));
		columnNames.add(journalTable.getVersionField());
		columnNames.add(journalTable.getSubsequentVersionField());

		relBuilder.project(sources, columnNames);

		relBuilder.insertCopying(
				tableModify,
				journalTable.getJournalTable()
		);

		return relBuilder.build();
	}
}
