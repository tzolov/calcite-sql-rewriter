package org.apache.calcite.adapter.jdbc;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Pair;

import io.pivotal.beach.calcite.programs.BasicForcedRule;

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

		RelNode input = tableModify.getInput();
		if (input instanceof LogicalValues) {

			// TODO: do we need to do anything here?
			relBuilder.push(input);

		} else if (input instanceof LogicalProject) {

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

		} else {
			throw new IllegalStateException("Unknown Calcite INSERT structure");
		}

		relBuilder.push(new LogicalTableModify(
				originalRel.getCluster(),
				tableModify.getTraitSet(),
				JdbcTableUtils.toRelOptTable(tableModify.getTable(), journalTable.getJournalTable()),
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
