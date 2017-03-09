package org.apache.calcite.adapter.jdbc;

import java.util.List;

import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcFilter;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcProject;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcTableModify;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

/**
 * Created by tzoloc on 11/25/16.
 */

public class MyTableUpdateRule extends RelOptRule {

	public MyTableUpdateRule() {
		super(operand(JdbcTableModify.class, any()));
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		JdbcTableModify jdbcTableModify = call.rel(0);
		return jdbcTableModify.getOperation().equals(Operation.UPDATE);
	}

	private void printNode(RelNode node, String offset) {
		if (node instanceof RelSubset) {
//			System.out.println(offset + node.getRelTypeName());
			node = ((RelSubset)node).getOriginal();
//			printNode(n, offset);
		}
		System.out.println(offset + node);
//			System.out.println(offset + RelOptUtil.toString(node));

			for (RelNode n : node.getInputs()) {
				printNode(n, offset + offset);
			}

	}
	@Override
	public void onMatch(RelOptRuleCall call) {
		JdbcTableModify update = call.rel(0);
		System.out.println("Expanded RelNode: \n" + RelOptUtil.toString(update));
		printNode(update, "  ");

		RelBuilder relBuilder = call.builder();
		RexBuilder rexBuilder = update.getCluster().getRexBuilder();

		RelOptCluster cluster = update.getCluster();
		RelTraitSet traitSet = update.getTraitSet();

		RelNode b = update.getInput();
//		JdbcTable table = ((JdbcTableScan) update.getInput().getTable()).jdbcTable;
//		Schema schema = JdbcTableUtilities.getSchema(table);
//		JournalledTableConfiguration tableConfig = JournalledTableConfiguration.get(
//				schema,
//				JdbcTableUtilities.getName(table)
//		);
//		if (tableConfig == null) {
//			// Not a journalled table; nothing to do
//			return;
//		}

		RelNode input = update.getInput();
		// 1. Input scan or select
		relBuilder.push(update.getInput());

		ImmutableList<RexNode> fields = relBuilder.fields();


		// 2. Over
		RelDataType type =  SqlStdOperatorTable.MAX.inferReturnType(
				relBuilder.getTypeFactory(), //new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT),
				ImmutableList.of(relBuilder.field("version_number").getType()));
		SqlAggFunction operator = SqlStdOperatorTable.MAX;
		List<RexNode> exprs = ImmutableList.of((RexNode)relBuilder.field("version_number"));
		List<RexNode> partitionKeys = ImmutableList.of((RexNode)relBuilder.field("deptno"));
		ImmutableList<RexFieldCollation> orderKeys = ImmutableList.of();

		RexWindowBound lowerBound = RexWindowBound.create(SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO), null);
		RexWindowBound upperBound = RexWindowBound.create(SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO), null);
		boolean physical = true;
		boolean allowPartial = true;
		boolean nullWhenCountZero = false;

		RexNode maxOverRexNode = rexBuilder.makeOver(type, operator, exprs, partitionKeys, orderKeys,
				lowerBound, upperBound, physical, allowPartial, nullWhenCountZero);

		ImmutableList<RexNode> newProjections = ImmutableList.<RexNode>builder().addAll(fields).add(maxOverRexNode).build();
		RelDataType newProjectDataType = relBuilder.getTypeFactory().builder()
				.addAll(input.getRowType().getFieldList())
				.add("last_version_number", maxOverRexNode.getType()).build();

		relBuilder.push(new LogicalProject(cluster, traitSet, input, newProjections, newProjectDataType));

		// 3. FILTER (exp_date = last_version_number)
		RexNode condition = relBuilder.call(SqlStdOperatorTable.EQUALS,
				relBuilder.field("version_number"),
				relBuilder.field("last_version_number"));

		JdbcFilter jdbcFilter = new JdbcFilter(cluster, traitSet, relBuilder.peek(), condition);
		relBuilder.push(jdbcFilter);

		// 4. TOP PROJECT (same as the Scan Row Type - filters out the last_version_number column)
		relBuilder.push(new LogicalProject(cluster, traitSet, jdbcFilter, fields, input.getRowType()));

		// 5. Insert
		JdbcTableModify insert = new JdbcTableModify(update.getCluster(),
				update.getTraitSet(),
				update.getTable(),
				update.getCatalogReader(),
				relBuilder.peek(),
				Operation.INSERT,
				null,
				null,
				false);

		relBuilder.push(insert);

		RelNode relNode = relBuilder.build();

		System.out.println("Expanded RelNode: \n" + RelOptUtil.toString(relNode));

		call.transformTo(relNode);
	}

	private class MyJdbcInsert extends JdbcTableModify {

		public MyJdbcInsert(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, CatalogReader catalogReader, RelNode input, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened) {
			super(cluster, traitSet, table, catalogReader, input, operation, updateColumnList, sourceExpressionList, flattened);
		}

		@Override
		public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
			return super.computeSelfCost(planner, mq).multiplyBy(.001);
		}
	}
}
