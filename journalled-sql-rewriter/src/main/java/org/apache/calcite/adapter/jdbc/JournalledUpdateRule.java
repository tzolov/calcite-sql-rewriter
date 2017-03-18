package org.apache.calcite.adapter.jdbc;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

public class JournalledUpdateRule extends AbstractForcedRule {

	public JournalledUpdateRule() {
		super(Operation.UPDATE);
	}

	@Override
	public RelNode doApply(LogicalTableModify tableModify, JournalledJdbcTable journalTable,
			JdbcRelBuilderFactory relBuilderFactory) {

		if (!(tableModify.getInput() instanceof LogicalProject)) {
			throw new IllegalStateException("Unknown Calcite UPDATE structure");
		}

		String versionField = journalTable.getVersionField();

		// Merge the Update's update column expression into the target INSERT
		LogicalProject project = (LogicalProject) tableModify.getInput();
		List<RexNode> desiredFields = new ArrayList<>();
		List<String> desiredNames = new ArrayList<>();

		for (Pair<RexNode, String> field : project.getNamedProjects()) {
			if (field.getKey() instanceof RexInputRef) {
				int index = tableModify.getUpdateColumnList().indexOf(field.getValue());
				if (index != -1) {
					desiredFields.add(tableModify.getSourceExpressionList().get(index));
				}
				else {
					desiredFields.add(field.getKey());
				}
				desiredNames.add(field.getValue());
			}
		}

		JdbcRelBuilder relBuilder = relBuilderFactory.create(
				tableModify.getCluster(),
				tableModify.getTable().getRelOptSchema()
		);

		relBuilder.push(project.getInput());

		JournalVersionType versionType = journalTable.getVersionType();
		if (!versionType.isValidSqlType(relBuilder.field(versionField).getType().getSqlTypeName())) {
			throw new IllegalStateException("Incorrect version_number type! Model's journalVersionType it: ["
					+ versionType.name() + "], But actual column type is: ["
					+ relBuilder.field(versionField).getType().getSqlTypeName() + "]. Verify if your journalVersionType " +
					"is correctly set!");
		}
		if (versionType.updateRequiresExplicitVersion()) {
			RexNode newVersion = versionType.incrementVersion(relBuilder, relBuilder.field(versionField));
			desiredFields.add(newVersion);
			desiredNames.add(versionField);
		}

		relBuilder.project(desiredFields, desiredNames);

		// Convert the UPDATE into INSERT TableModify operations
		relBuilder.insertCopying(
				tableModify,
				journalTable.getJournalTable()
		);

		return relBuilder.build();
	}
}
