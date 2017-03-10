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

		// Bypass the projection
		tableModify.replaceInput(0, tableModify.getInput(0).getInput(0));

		return tableModify;
	}
}
