package org.apache.calcite.adapter.jdbc;

import io.pivotal.beach.calcite.programs.BasicForcedRule;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class JournalledUpdateRule implements BasicForcedRule {
	@Override
	public RelNode apply(RelNode originalRel, JdbcRelBuilderFactory relBuilderFactory) {
		if (!(originalRel instanceof LogicalTableModify)) {
			return null;
		}

		LogicalTableModify tableModify = (LogicalTableModify) originalRel;

		if (!tableModify.isUpdate()) {
			return null;
		}

		JdbcTable jdbcTable = JdbcTableUtils.getJdbcTable(tableModify);
		if (!(jdbcTable instanceof JournalledJdbcTable)) {
			// Not a journal table; nothing to do
			return null;
		}
		JournalledJdbcTable journalTable = (JournalledJdbcTable) jdbcTable;

		if (!(tableModify.getInput() instanceof LogicalProject)) {
			throw new IllegalStateException("Unknown Calcite UPDATE structure");
		}

		// Merge the Update's update column expression into the target INSERT
		LogicalProject project = (LogicalProject) tableModify.getInput();
		List<RexNode> desiredFields = new ArrayList<>();
		List<String> desiredNames = new ArrayList<>();

		for (Pair<RexNode, String> field : project.getNamedProjects()) {
			if (field.getKey() instanceof RexInputRef) {
				int index = tableModify.getUpdateColumnList().indexOf(field.getValue());
				if (index >=0 ) {
					desiredFields.add(tableModify.getSourceExpressionList().get(index));
				} else {
					desiredFields.add(field.getKey());
				}
				desiredNames.add(field.getValue());
			}
		}

		JdbcRelBuilder relBuilder = relBuilderFactory.create(
				originalRel.getCluster(),
				tableModify.getTable().getRelOptSchema()
		);

		relBuilder.push(project.getInput());
		relBuilder.project(desiredFields, desiredNames);

		// Convert the UPDATE into INSERT TableModify operations
		TableModify newTableModify = journalTable.toModificationRel(
				originalRel.getCluster(),
				JdbcTableUtils.toRelOptTable(tableModify.getTable(), journalTable),
				tableModify.getCatalogReader(),
				relBuilder.peek(),
				Operation.INSERT,
				null,
				null,
				tableModify.isFlattened()
		);

		relBuilder.push(newTableModify);

		return relBuilder.build();
	}
}
