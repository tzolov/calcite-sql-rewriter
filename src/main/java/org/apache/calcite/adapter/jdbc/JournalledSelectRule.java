package org.apache.calcite.adapter.jdbc;

import com.google.common.collect.ImmutableList;
import io.pivotal.beach.calcite.programs.BasicForcedRule;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * Created by tzoloc on 11/25/16.
 */

public class JournalledSelectRule implements BasicForcedRule {
	@Override
	public RelNode apply(RelNode originalScan, JdbcRelBuilderFactory relBuilderFactory) {
		if (!(originalScan instanceof JdbcTableScan)) {
			return null;
		}

		JdbcTable table = ((JdbcTableScan) originalScan).jdbcTable;
		if (!(table instanceof JournalledJdbcTable)) {
			// Not a journalled table; nothing to do
			return null;
		}
		JournalledJdbcTable journalledTable = (JournalledJdbcTable) table;

		JdbcRelBuilder relBuilder = relBuilderFactory.create(
				originalScan.getCluster(),
				originalScan.getTable().getRelOptSchema()
		);

		// FROM <table_journal>
		relBuilder.scanJdbc(journalledTable.getJournalTable());

		RexInputRef versionField = relBuilder.field(journalledTable.getVersionField());
		RexInputRef subsequentVersionField = relBuilder.field(journalledTable.getSubsequentVersionField());

		// <maxVersionField> = MAX(<version_number>) OVER (PARTITION BY <primary_key>)
		RexInputRef maxVersionField = relBuilder.appendField(relBuilder.makeOver(
				SqlStdOperatorTable.MAX,
				ImmutableList.of(versionField),
				relBuilder.fields(journalledTable.getKeys())
		));

		// WHERE <version_field> = <maxVersionField> AND <subsequent_version_field> IS NULL
		relBuilder.filter(
				relBuilder.equals(versionField, maxVersionField),
				relBuilder.isNull(subsequentVersionField)
		);

		// SELECT <originally_requested_columns>
		relBuilder.projectToMatch(originalScan.getRowType());

		return relBuilder.build();
	}
}
