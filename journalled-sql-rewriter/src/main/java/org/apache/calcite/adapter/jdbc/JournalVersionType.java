package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

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

		@Override
		public boolean isValidSqlType(SqlTypeName sqlTypeName) {
			return SqlTypeName.TIMESTAMP.equals(sqlTypeName);
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

		@Override
		public boolean isValidSqlType(SqlTypeName sqlTypeName) {
			return sqlTypeName.NUMERIC_TYPES.contains(sqlTypeName);
		}
	};

	abstract public RexNode incrementVersion(JdbcRelBuilder relBuilder, RexNode previousVersion);
	abstract public boolean updateRequiresExplicitVersion();
	abstract public boolean isValidSqlType(SqlTypeName sqlTypeName);
}
