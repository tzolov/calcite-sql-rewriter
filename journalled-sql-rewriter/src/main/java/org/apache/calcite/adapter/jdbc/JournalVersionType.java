package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public enum JournalVersionType {
	TIMESTAMP {
		@Override
		public RexNode incrementVersion(JdbcRelBuilder relBuilder, RexNode previousVersion) {
			return relBuilder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP);
		}

		@Override
		public boolean updateRequiresExplicitVersion() {
			return false;
		}
	},
	BIGINT {
		@Override
		public RexNode incrementVersion(JdbcRelBuilder relBuilder, RexNode previousVersion) {
			return relBuilder.call(SqlStdOperatorTable.PLUS, previousVersion, relBuilder.literal(1));
		}

		@Override
		public boolean updateRequiresExplicitVersion() {
			return true;
		}
	};

	abstract public RexNode incrementVersion(JdbcRelBuilder relBuilder, RexNode previousVersion);
	abstract public boolean updateRequiresExplicitVersion();
}
