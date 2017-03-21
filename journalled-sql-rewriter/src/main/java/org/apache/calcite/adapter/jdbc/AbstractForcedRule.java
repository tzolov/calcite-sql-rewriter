/*
 * Copyright 2012-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.schema.Table;

import org.apache.calcite.adapter.jdbc.programs.ForcedRule;

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
