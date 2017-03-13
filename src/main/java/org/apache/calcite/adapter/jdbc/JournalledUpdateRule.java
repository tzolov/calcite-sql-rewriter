package org.apache.calcite.adapter.jdbc;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactory;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import io.pivotal.beach.calcite.programs.BasicForcedRule;

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

		if (!(tableModify.getInput() instanceof LogicalProject)) {
			return null;
		}

		//TODO
		// Add check that to ignore non-journal tables

		// TODO Explore for a better approach!
		//Use Java reflexion (and implementation specific classes) to retrieve the JournalledJdbcTable.
		JournalledJdbcTable journalTable =
				(JournalledJdbcTable) JournalledUpdateRule.get((RelOptTableImpl) tableModify.getTable(), "table");


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

		List<String> names = JdbcTableUtils.getQualifiedName(tableModify.getTable(), journalTable);
		RelOptTable relOptJournalTable = tableModify.getTable().getRelOptSchema().getTableForMember(names);

		// Convert the UPDATE into INSERT TableModify operations
		TableModify newTableModify = journalTable.toModificationRel(
				originalRel.getCluster(),
				relOptJournalTable,
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

	private static Object get(RelOptTableImpl relOptTable, String fieldName) {
		try {
			Field field = RelOptTableImpl.class.getDeclaredField(fieldName);
			field.setAccessible(true);
			return field.get(relOptTable);
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
}
