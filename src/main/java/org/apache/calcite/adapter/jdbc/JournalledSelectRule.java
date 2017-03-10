package org.apache.calcite.adapter.jdbc;

import com.google.common.collect.ImmutableList;
import io.pivotal.beach.calcite.programs.BasicForcedRule;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by tzoloc on 11/25/16.
 */

public class JournalledSelectRule implements BasicForcedRule {
	@Override
	public RelNode apply(RelNode originalScan, RelBuilderFactory relBuilderFactory) {
		if(!(originalScan instanceof JdbcTableScan)) {
			return null;
		}

		JdbcTable table = ((JdbcTableScan) originalScan).jdbcTable;
		if(!(table instanceof JournalledJdbcTable)) {
			// Not a journalled table; nothing to do
			return null;
		}
		JournalledJdbcTable journalledTable = (JournalledJdbcTable) table;

		RelOptSchema relOptSchema = originalScan.getTable().getRelOptSchema();

		RelOptCluster cluster = originalScan.getCluster();
		RelBuilder relBuilder = relBuilderFactory.create(cluster, relOptSchema);

		// FROM <table_journal>

		String journalName = JdbcTableUtils.getTableName(journalledTable.getJournalTable());
		List<String> fqJournalName = new ArrayList<>(originalScan.getTable().getQualifiedName());
		fqJournalName.set(fqJournalName.size() - 1, journalName);

		relBuilder.push(JdbcTableUtils.makeTableScan(
				cluster,
				relOptSchema.getTableForMember(fqJournalName),
				journalledTable.getJournalTable()
		));

		RexInputRef versionField = relBuilder.field(journalledTable.getVersionField());
		RexInputRef subsequentVersionField = relBuilder.field(journalledTable.getSubsequentVersionField());

		// <maxVersionField> = MAX(<version_number>) OVER (PARTITION BY <primary_key>)
		RexInputRef maxVersionField = BuilderUtils.appendField(relBuilder, BuilderUtils.makeOver(
				relBuilder,
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
		BuilderUtils.projectToMatch(relBuilder, originalScan.getRowType());

		return relBuilder.build();
	}
}
