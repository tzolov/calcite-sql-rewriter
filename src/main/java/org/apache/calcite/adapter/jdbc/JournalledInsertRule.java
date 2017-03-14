package org.apache.calcite.adapter.jdbc;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Pair;

/**
 * Created by tzoloc on 11/25/16.
 */

public class JournalledInsertRule extends AbstractBasicFourceRuleRule {

	public JournalledInsertRule() {
		super(Operation.INSERT);
	}

	@Override
	public RelNode doApply(LogicalTableModify tableModify, JournalledJdbcTable journalTable,
			JdbcRelBuilderFactory relBuilderFactory) {

		JdbcRelBuilder relBuilder = relBuilderFactory.create(
				tableModify.getCluster(),
				tableModify.getTable().getRelOptSchema()
		);

		RelNode input = tableModify.getInput();
		if (input instanceof LogicalValues) {

			// TODO: do we need to do anything here?
			relBuilder.push(input);

		}
		else if (input instanceof LogicalProject) {

			LogicalProject project = (LogicalProject) input;
			List<RexNode> desiredFields = new ArrayList<>();
			List<String> desiredNames = new ArrayList<>();
			for (Pair<RexNode, String> field : project.getNamedProjects()) {
				if (field.getKey() instanceof RexInputRef) {
					desiredFields.add(field.getKey());
					desiredNames.add(field.getValue());
				}
			}

			relBuilder.push(project.getInput());
			relBuilder.project(desiredFields, desiredNames);

		}
		else {
			throw new IllegalStateException("Unknown Calcite INSERT structure");
		}

		relBuilder.insertCopying(
				tableModify,
				journalTable.getJournalTable()
		);

		return relBuilder.build();

	}
}
