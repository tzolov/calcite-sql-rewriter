package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class MyJdbcProject extends JdbcRules.JdbcProject {
	MyJdbcProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
		super(cluster, traitSet, input, projects, rowType);
	}

	@Override
	public JdbcRules.JdbcProject copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
		return new MyJdbcProject(getCluster(), traitSet, input, projects, rowType);
	}
}
