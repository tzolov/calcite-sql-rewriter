package org.apache.calcite.adapter.jdbc;

import io.pivotal.beach.calcite.programs.BasicForcedRule;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.List;

public class JournalledDeleteRule implements BasicForcedRule {
	@Override
	public RelNode apply(RelNode originalRel, JdbcRelBuilderFactory relBuilderFactory) {
		if (!(originalRel instanceof LogicalTableModify)) {
			return null;
		}

		LogicalTableModify tableModify = (LogicalTableModify) originalRel;

		if (!tableModify.getOperation().equals(TableModify.Operation.DELETE)) {
			return null;
		}

		JdbcRelBuilder relBuilder = relBuilderFactory.create(originalRel.getCluster(), tableModify.getTable().getRelOptSchema());
		relBuilder.push(tableModify.getInput());

		List<String> columnNames = new ArrayList<>();
		List<RexNode> sources = new ArrayList<>();
		for(RexNode n : relBuilder.fields()) {
			sources.add(n);
			columnNames.add(null);
		}

		// TODO: get these from the configuration
		sources.add(relBuilder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP));
		columnNames.add("version_number");
		sources.add(relBuilder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP));
		columnNames.add("subsequent_version_number");

		relBuilder.project(sources, columnNames);

		List<String> journalName = new ArrayList<>();
		journalName.addAll(tableModify.getTable().getQualifiedName());
		journalName.set(journalName.size() - 1, journalName.get(journalName.size() - 1) + "_journal"); // TODO: get this from the configuration

		relBuilder.push(new LogicalTableModify(
				originalRel.getCluster(),
				tableModify.getTraitSet(),
				tableModify.getTable().getRelOptSchema().getTableForMember(journalName),
				tableModify.getCatalogReader(),
				relBuilder.peek(),
				TableModify.Operation.INSERT,
				null,
				null,
				tableModify.isFlattened()
		));

		return relBuilder.build();
	}
}
