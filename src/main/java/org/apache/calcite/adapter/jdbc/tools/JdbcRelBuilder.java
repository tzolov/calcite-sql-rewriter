package org.apache.calcite.adapter.jdbc.tools;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.jdbc.JdbcTableUtils;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"WeakerAccess", "SameParameterValue"}) // Public API
public class JdbcRelBuilder extends RelBuilder {
	private JdbcRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
		super(context, cluster, relOptSchema);
	}

	public RexNode makeOver(
			SqlAggFunction operator,
			List<RexNode> exprs,
			List<RexNode> partitionKeys
	) {
		return makeOver(
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

	public RexNode makeOver(
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
		for (RexNode n : exprs) {
			types.add(n.getType());
		}
		return getRexBuilder().makeOver(
				operator.inferReturnType(getTypeFactory(), types),
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

	public JdbcRelBuilder projectToMatch(RelDataType row) {
		List<String> originalFieldNames = row.getFieldNames();
		List<RexNode> mappedFields = new ArrayList<>();
		for (String fieldName : originalFieldNames) {
			mappedFields.add(field(fieldName));
		}
		project(mappedFields);
		convert(row, true); // Patch nullability of columns, etc.
		return this;
	}

	public RexInputRef appendField(RexNode field) {
		List<RexNode> fields = new ArrayList<>();
		fields.addAll(fields());
		fields.add(field);
		project(fields);
		return field(fields.size() - 1);
	}

	// Table MUST be a JdbcTable (cannot be typesafe since JdbcTable is package-private)
	public JdbcRelBuilder scanJdbc(Table table) {
		push(JdbcTableUtils.toRel(cluster, relOptSchema, table));
		return this;
	}

	public static class Factory implements JdbcRelBuilderFactory {
		private Context context;

		private Factory(Context context) {
			this.context = context;
		}

		@Override
		public JdbcRelBuilder create(RelOptCluster cluster, RelOptSchema schema) {
			return new JdbcRelBuilder(context, cluster, schema);
		}
	}

	public static class FactoryFactory implements JdbcRelBuilderFactoryFactory {
		@Override
		public JdbcRelBuilderFactory create(Context context) {
			return new JdbcRelBuilder.Factory(context);
		}
	}
}
