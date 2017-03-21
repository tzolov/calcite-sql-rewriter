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
