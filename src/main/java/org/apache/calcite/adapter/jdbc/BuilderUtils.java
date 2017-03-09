package org.apache.calcite.adapter.jdbc;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;

public class BuilderUtils {
	public static RexNode makeOver(
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

	public static RexNode makeOver(
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
}
