package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlIdentifier;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("WeakerAccess") // Public API
public class JdbcTableUtils {
	private JdbcTableUtils() {
		throw new UnsupportedOperationException();
	}

	private static Object get(JdbcTable table, String fieldName) {
		try {
			Field field = JdbcTable.class.getDeclaredField(fieldName);
			field.setAccessible(true);
			return field.get(table);
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	static String getCatalogName(JdbcTable table) {
		return (String) get(table, "jdbcCatalogName");
	}

	static String getSchemaName(JdbcTable table) {
		return (String) get(table, "jdbcSchemaName");
	}

	static String getTableName(JdbcTable table) {
		// tableName is: [catalog,] [schema,] table
		SqlIdentifier identifier = table.tableName();
		return identifier.names.get(identifier.names.size() - 1);
	}

	static List<String> getQualifiedName(RelOptTable sibling, JdbcTable table) {
		List<String> name = new ArrayList<>();
		if(sibling != null) {
			name.addAll(sibling.getQualifiedName());
			name.remove(name.size() - 1);
		}
		name.add(getTableName(table));
		return name;
	}

	static RelNode toRel(RelOptCluster cluster, RelOptSchema relOptSchema, JdbcTable table, List<String> qualifiedName) {
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

		return table.toRel(
				toRelContext,
				relOptSchema.getTableForMember(qualifiedName)
		);
	}

	public static RelNode toRel(RelOptCluster cluster, RelOptSchema relOptSchema, Table table, List<String> qualifiedName) {
		if(!(table instanceof JdbcTable)) {
			throw new UnsupportedOperationException();
		}
		return toRel(cluster, relOptSchema, (JdbcTable) table, qualifiedName);
	}
}
