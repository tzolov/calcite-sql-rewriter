package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import io.pivotal.beach.calcite.programs.BasicForcedRule;

/**
 * Created by tzoloc on 11/25/16.
 */

public class JournalledInsertRule implements BasicForcedRule {
	@Override
	public RelNode apply(RelNode originalRel, RelBuilderFactory relBuilderFactory) {

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
		LogicalValues logicalValues = (LogicalValues) logicalProject.getInput();

		RelOptSchema relOptSchema = originalRel.getTable().getRelOptSchema();

		RelOptCluster cluster = originalRel.getCluster();

		RelBuilder relBuilder = relBuilderFactory.create(cluster, relOptSchema);

		relBuilder.values(logicalValues.getTuples(), logicalValues.getRowType());

		LogicalTableModify newTableModify = LogicalTableModify.create(
				tableModify.getTable(),
				tableModify.getCatalogReader(),
				relBuilder.peek(),
				Operation.INSERT,
				null, //List < String > updateColumnList,
				null, //List < RexNode > sourceExpressionList,
				tableModify.isFlattened()
		);

		relBuilder.push(newTableModify);

		return relBuilder.build();
	}
}
