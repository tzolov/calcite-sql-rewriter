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
package org.apache.calcite.adapter.jdbc.tools;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.adapter.jdbc.JdbcTableUtils;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

@SuppressWarnings({"WeakerAccess", "SameParameterValue"}) // Public API
public class JdbcRelBuilder extends RelBuilder {
	private JdbcRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
		super(context, cluster, relOptSchema);
	}

	public RexNode makeOver(
			SqlAggFunction operator,
			List<RexNode> expressions,
			List<RexNode> partitionKeys
	) {
		final Set<SqlKind> flags = EnumSet.noneOf(SqlKind.class);

		// TODO
		// This is a temporal fix to make HAWQ work with OVER + UNLIMITED BOUNDS
		// HAWQ requires ORDER BY if andy BOUNDS are set even unlimited upper and lower BOUNDS (which is equal to
		// the entire partition - e.g. not setting BOUNDs at all --
		// Note that the unnecessary ORDER BY have negative performance impact and has to be remove once either HAWQ
		// start supporting unbounded bounds without order by or Calcite can generate shorthand OVER PARTITION BY
		// syntax.
		List<RexFieldCollation> orderKeys = expressions.stream().map(
				rexNode -> new RexFieldCollation(rexNode, flags)).collect(Collectors.toList());

		return makeOver(
				operator,
				expressions,
				partitionKeys,
				ImmutableList.copyOf(orderKeys),
				RexWindowBound.create(SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO), null),
				RexWindowBound.create(SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO), null),
				true,
				true,
				false
		);
	}

	public RexNode makeOver(
			SqlAggFunction operator,
			List<RexNode> expressions,
			List<RexNode> partitionKeys,
			ImmutableList<RexFieldCollation> orderKeys, // Calcite API is weird
			RexWindowBound lowerBound,
			RexWindowBound upperBound,
			boolean physical,
			boolean allowPartial,
			boolean nullWhenCountZero
	) {
		List<RelDataType> types = new ArrayList<>(expressions.size());
		for (RexNode n : expressions) {
			types.add(n.getType());
		}
		return getRexBuilder().makeOver(
				operator.inferReturnType(getTypeFactory(), types),
				operator,
				expressions,
				partitionKeys,
				orderKeys,
				lowerBound,
				upperBound,
				physical,
				allowPartial,
				nullWhenCountZero
		);
	}

	public RexInputRef appendField(RexNode field) {
		List<RexNode> fields = new ArrayList<>();
		fields.addAll(fields());
		fields.add(field);
		project(fields);
		return field(fields.size() - 1);
	}

	// Table MUST be a JdbcTable (cannot be type-safe since JdbcTable is package-private)
	public JdbcRelBuilder scanJdbc(Table table, List<String> qualifiedName) {
		push(JdbcTableUtils.toRel(cluster, relOptSchema, table, qualifiedName));
		return this;
	}

	public JdbcRelBuilder insertCopying(
			LogicalTableModify original,
			Table table
	) {
		List<String> name = JdbcTableUtils.getQualifiedName(original.getTable(), table);

		push(new LogicalTableModify(
				cluster,
				original.getTraitSet(),
				relOptSchema.getTableForMember(name),
				original.getCatalogReader(),
				peek(),
				TableModify.Operation.INSERT,
				null,
				null,
				original.isFlattened()
		));
		return this;
	}

	public static class Factory implements JdbcRelBuilderFactory {
		private Context context;

		public Factory(Context context) {
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
