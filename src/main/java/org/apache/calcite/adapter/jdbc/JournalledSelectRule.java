package org.apache.calcite.adapter.jdbc;

import com.google.common.collect.ImmutableList;
import io.pivotal.beach.calcite.configuration.JournalledTableConfiguration;
import io.pivotal.beach.calcite.programs.BasicForcedRule;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.schema.Path;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by tzoloc on 11/25/16.
 */

public class JournalledSelectRule implements BasicForcedRule {
	RexNode makeOver(
			RelBuilder relBuilder,
			SqlAggFunction operator,
			List<RexNode> exprs,
			List<RexNode> partitionKeys
	) {
		return makeOver(
				relBuilder,
				operator,
				exprs,
				partitionKeys,
				ImmutableList.of(),
				RexWindowBound.create(SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO), null),
				RexWindowBound.create(SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO), null),
				true,
				true,
				false
		);
	}

	RexNode makeOver(
			RelBuilder relBuilder,
			SqlAggFunction operator,
			List<RexNode> exprs,
			List<RexNode> partitionKeys,
			ImmutableList<RexFieldCollation> orderKeys, // Calcite API is weird
			RexWindowBound lowerBound,
			RexWindowBound upperBound,
			boolean physical,
			boolean allowPartial,
			boolean nullWhenCountZero
	) {
		List<RelDataType> types = new ArrayList<>(exprs.size());
		for(RexNode n : exprs) {
			types.add(n.getType());
		}
		return relBuilder.getRexBuilder().makeOver(
				operator.inferReturnType(relBuilder.getTypeFactory(), types),
				operator,
				exprs,
				partitionKeys,
				orderKeys,
				lowerBound,
				upperBound,
				physical,
				allowPartial,
				nullWhenCountZero
		);
	}

	@Override
	public RelNode apply(RelNode originalScan, RelBuilderFactory relBuilderFactory) {
		if(!(originalScan instanceof JdbcTableScan)) {
			return null;
		}

		JdbcTable table = ((JdbcTableScan) originalScan).jdbcTable;
		RelOptSchema relOptSchema = originalScan.getTable().getRelOptSchema();
		Schema schema = JdbcTableUtilities.getSchema(table);

		JournalledTableConfiguration tableConfig = JournalledTableConfiguration.get(
				schema,
				JdbcTableUtilities.getName(table)
		);
		if (tableConfig == null) {
			// Not a journalled table; nothing to do
			// (should never happen since we filter in matches)
			return null;
		}

		JdbcTable journalTable = (JdbcTable) schema.getTable(tableConfig.getJournal());

		RelOptCluster cluster = originalScan.getCluster();
		RelTraitSet traitSet = originalScan.getTraitSet();
		RelBuilder relBuilder = relBuilderFactory.create(cluster, relOptSchema);

		// FROM <table_journal>
		relBuilder.scan(journalTable.tableName().names);

		ImmutableList<RexNode> allJournalFields = relBuilder.fields();
		RexInputRef versionField = relBuilder.field(tableConfig.getVersionField());
		RexInputRef subsequentVersionField = relBuilder.field(tableConfig.getSubsequentVersionField());

		// MAX(<version_number>) OVER (PARTITION BY <primary_key>)
		RexNode maxVersionField = makeOver(
				relBuilder,
				SqlStdOperatorTable.MAX,
				ImmutableList.of(versionField),
				relBuilder.fields(tableConfig.getKeys())
		);

		// SELECT *, <maxVersionField>
		relBuilder.project(
				ImmutableList.<RexNode>builder()
						.addAll(allJournalFields)
						.add(maxVersionField)
						.build()
		);

		// TODO: oops, I broke these
//		// WHERE <version_field> = <maxVersionField>
//		relBuilder.equals(versionField, maxVersionField);
//		// WHERE <subsequent_version_field> IS NULL
//		relBuilder.isNull(subsequentVersionField);

		// SELECT <originally_requested_columns>
//		relBuilder.project(originalScan.getRowType()); // ??
		relBuilder.push(new JdbcRules.JdbcProject(cluster, traitSet, relBuilder.peek(), allJournalFields, originalScan.getRowType()));

		RelNode relNode = relBuilder.build();

		System.out.println("Expanded RelNode: \n" + RelOptUtil.toString(relNode, SqlExplainLevel.ALL_ATTRIBUTES));

		return relNode;
	}
}
