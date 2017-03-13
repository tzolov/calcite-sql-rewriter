package org.apache.calcite.adapter.jdbc;

import io.pivotal.beach.calcite.programs.BasicForcedRule;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

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

		if (!tableModify.isInsert()) {
			return null;
		}

		RelNode input = tableModify.getInput();
		if (!(input instanceof LogicalProject)) {
			return null;
		}

		LogicalProject project = (LogicalProject) input;
		List<RexNode> desiredFields = new ArrayList<>();
		List<String> desiredNames = new ArrayList<>();
		for (Pair<RexNode, String> field : project.getNamedProjects()) {
			if (field.getKey() instanceof RexInputRef) {
				desiredFields.add(field.getKey());
				desiredNames.add(field.getValue());
			}
		}

		JdbcRelBuilder relBuilder = relBuilderFactory.create(
				originalRel.getCluster(),
				tableModify.getTable().getRelOptSchema()
		);

		relBuilder.push(project.getInput());
		RelNode newProjection = relBuilder.project(desiredFields, desiredNames).build();
		tableModify.replaceInput(0, newProjection);

		return tableModify;
	}
}
