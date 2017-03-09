package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcTableModify;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.collect.ImmutableList;
import io.pivotal.beach.calcite.configuration.JournalledTableConfiguration;
import io.pivotal.beach.calcite.programs.BasicForcedRule;

/**
 * Created by tzoloc on 11/25/16.
 */

public class JournalledInsertRule implements BasicForcedRule {
	@Override
	public RelNode apply(RelNode originalRel, RelBuilderFactory relBuilderFactory) {
		if(!(originalRel instanceof LogicalTableModify)) {
			return null;
		}

		LogicalTableModify tableModify = (LogicalTableModify) originalRel;
		if (!tableModify.getOperation().equals(Operation.INSERT)) {
			return null;
		}

		return originalRel;
	}
}
