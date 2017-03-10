package org.apache.calcite.adapter.jdbc;

import io.pivotal.beach.calcite.programs.BasicForcedRule;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactory;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;

/**
 * Created by tzoloc on 11/25/16.
 */

public class JournalledInsertRule implements BasicForcedRule {
	@Override
	public RelNode apply(RelNode originalRel, JdbcRelBuilderFactory relBuilderFactory) {

		if (!(originalRel instanceof LogicalTableModify)) {
			return null;
		}

		LogicalTableModify tableModify = (LogicalTableModify) originalRel;

		if (!tableModify.getOperation().equals(Operation.INSERT)) {
			return null;
		}

		if (!(tableModify.getInput() instanceof LogicalProject)) {
			return null;
		}

		LogicalProject logicalProject = (LogicalProject) tableModify.getInput();

		JdbcRelBuilder relBuilder = relBuilderFactory.create(
				originalRel.getCluster(),
				originalRel.getTable().getRelOptSchema()
		);

		relBuilder.push(logicalProject.getInput());

		RelOptTable journalTable = tableModify.getTable();

		LogicalTableModify newTableModify = LogicalTableModify.create(
				journalTable,
				tableModify.getCatalogReader(),
				relBuilder.peek(),
				Operation.INSERT,
				null, //List < String > updateColumnList,
				null, //List < RexNode > sourceExpressionList,
				tableModify.isFlattened()
		);

		relBuilder.push(newTableModify);

		// SELECT <originally_requested_columns>
		relBuilder.projectToMatch(originalRel.getRowType());

		return relBuilder.build();
	}
}
