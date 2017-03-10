package org.apache.calcite.adapter.jdbc.tools;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.tools.RelBuilderFactory;

public interface JdbcRelBuilderFactory extends RelBuilderFactory {
	JdbcRelBuilder create(RelOptCluster cluster, RelOptSchema schema);
}
