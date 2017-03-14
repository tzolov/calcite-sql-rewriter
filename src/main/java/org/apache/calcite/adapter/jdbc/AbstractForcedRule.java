package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.schema.Table;

import io.pivotal.beach.calcite.programs.ForcedRule;

public abstract class AbstractForcedRule implements ForcedRule {

	private final LogicalTableModify.Operation operation;

	protected AbstractForcedRule(Operation operation) {
		this.operation = operation;
	}

	@Override
	public RelNode apply(RelNode originalRel, JdbcRelBuilderFactory relBuilderFactory) {

		if (!(originalRel instanceof LogicalTableModify)) {
			return null;
		}

		LogicalTableModify tableModify = (LogicalTableModify) originalRel;

		if (tableModify.getOperation() != operation) {
			return null;
		}

		Table baseTable = JdbcTableUtils.getJdbcTable(tableModify);
		if (!(baseTable instanceof JournalledJdbcTable)) {
			// Not a journal table; nothing to do
			return null;
		}
		JournalledJdbcTable journalTable = (JournalledJdbcTable) baseTable;

		return doApply(tableModify, journalTable, relBuilderFactory);
	}

	abstract public RelNode doApply(
			LogicalTableModify tableModify, JournalledJdbcTable journalTable,
			JdbcRelBuilderFactory relBuilderFactory);
}
