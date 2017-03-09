package org.apache.calcite.adapter.jdbc;

import com.google.common.collect.ImmutableList;
import io.pivotal.beach.calcite.configuration.JournalledTableConfiguration;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlIdentifier;

import java.lang.reflect.Field;
import java.util.List;

public class JdbcTableUtilities {
	public static Schema getSchema(JdbcTable table) {
		try {
			Field schemaF = JdbcTable.class.getDeclaredField("jdbcSchema");
			schemaF.setAccessible(true);
			return (Schema) schemaF.get(table);
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	public static String getName(JdbcTable table) {
		// tableName is: [catalog,] [schema,] table
		SqlIdentifier identifier = table.tableName();
		return identifier.names.get(identifier.names.size() - 1);
	}

	public static RelNode makeTableScan(RelOptCluster cluster, RelOptTable relOptTable, JdbcTable table) {
		RelOptTable.ToRelContext toRelContext = new RelOptTable.ToRelContext() {
			@Override
			public RelOptCluster getCluster() {
				return cluster;
			}

			@Override
			public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
				throw new UnsupportedOperationException();
			}
		};
		return table.toRel(toRelContext, relOptTable);
	}

	public static JournalledTableConfiguration configurationForTable(JdbcTable jdbcTable) {
		return JournalledTableConfiguration.get(
				JdbcTableUtilities.getSchema(jdbcTable),
				JdbcTableUtilities.getName(jdbcTable)
		);
	}
}
