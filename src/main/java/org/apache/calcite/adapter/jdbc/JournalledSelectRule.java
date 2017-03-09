package org.apache.calcite.adapter.jdbc;

import com.google.common.collect.ImmutableList;
import io.pivotal.beach.calcite.configuration.JournalledTableConfiguration;
import io.pivotal.beach.calcite.programs.BasicForcedRule;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.schema.Schema;
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
		RelOptSchema relOptSchema = originalScan.getTable().getRelOptSchema();
		Schema schema = JdbcTableUtilities.getSchema(table);

		JournalledTableConfiguration tableConfig = JdbcTableUtilities.configurationForTable(table);
		if (tableConfig == null) {
			// Not a journalled table; nothing to do
			return null;
		}

		RelOptCluster cluster = originalScan.getCluster();
		RelBuilder relBuilder = relBuilderFactory.create(cluster, relOptSchema);

		// FROM <table_journal>

		String journalName = tableConfig.getJournal();
		List<String> fqJournalName = new ArrayList<>(originalScan.getTable().getQualifiedName());
		fqJournalName.set(fqJournalName.size() - 1, journalName);

		relBuilder.push(JdbcTableUtilities.makeTableScan(
				cluster,
				relOptSchema.getTableForMember(fqJournalName),
				(JdbcTable) schema.getTable(journalName)
		));

		RexInputRef versionField = relBuilder.field(tableConfig.getVersionField());
		RexInputRef subsequentVersionField = relBuilder.field(tableConfig.getSubsequentVersionField());

		// <maxVersionField> = MAX(<version_number>) OVER (PARTITION BY <primary_key>)
		RexInputRef maxVersionField = BuilderUtils.appendField(relBuilder, BuilderUtils.makeOver(
				relBuilder,
				SqlStdOperatorTable.MAX,
				ImmutableList.of(versionField),
				relBuilder.fields(tableConfig.getKeys())
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
