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

import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactory;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.google.common.collect.ImmutableList;

import java.util.List;

class JournalledJdbcTable extends JdbcTable {
	private final JdbcTable journalTable;
	private final JournalledJdbcSchema journalledJdbcSchema;
	private final ImmutableList<String> keyColumnNames;
	JdbcRelBuilderFactory relBuilderFactory = new JdbcRelBuilder.Factory(null);

	JournalledJdbcTable(
			String tableName,
			JournalledJdbcSchema journalledJdbcSchema,
			JdbcTable journalTable,
			String[] keyColumnNames
	) {
		super(
				journalledJdbcSchema,
				JdbcTableUtils.getCatalogName(journalTable),
				JdbcTableUtils.getSchemaName(journalTable),
				tableName,
				journalTable.getJdbcTableType()
		);
		this.journalTable = journalTable;
		this.journalledJdbcSchema = journalledJdbcSchema;
		this.keyColumnNames = ImmutableList.copyOf(keyColumnNames);
	}

	String getVersionField() {
		return journalledJdbcSchema.getVersionField();
	}

	String getSubsequentVersionField() {
		return journalledJdbcSchema.getSubsequentVersionField();
	}

	JdbcTable getJournalTable() {
		return journalTable;
	}

	List<String> getKeyColumnNames() {
		return keyColumnNames;
	}

	JournalVersionType getVersionType() {
		return journalledJdbcSchema.getVersionType();
	}

	@Override
	public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
		JdbcRelBuilder relBuilder = relBuilderFactory.create(
				context.getCluster(),
				relOptTable.getRelOptSchema()
		);

		// FROM <table_journal>
		relBuilder.scanJdbc(
				getJournalTable(),
				JdbcTableUtils.getQualifiedName(relOptTable, getJournalTable())
		);

		RexInputRef versionField = relBuilder.field(getVersionField());
		RexInputRef subsequentVersionField = relBuilder.field(getSubsequentVersionField());

		// <maxVersionField> = MAX(<version_number>) OVER (PARTITION BY <primary_key>)
		RexInputRef maxVersionField = relBuilder.appendField(relBuilder.makeOver(
				SqlStdOperatorTable.MAX,
				ImmutableList.of(versionField),
				relBuilder.fields(getKeyColumnNames())
		));

		// WHERE <version_field> = <maxVersionField> AND <subsequent_version_field> IS NULL
		relBuilder.filter(
				relBuilder.equals(versionField, maxVersionField),
				relBuilder.isNull(subsequentVersionField)
		);

		return relBuilder.build();
	}
}
