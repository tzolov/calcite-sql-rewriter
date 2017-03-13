package org.apache.calcite.adapter.jdbc;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlIdentifier;

@SuppressWarnings("WeakerAccess") // Public API
public class JdbcTableUtils {
	private JdbcTableUtils() {
		throw new UnsupportedOperationException();
	}

	private static Object get(Class<?> clazz, Object object, String fieldName) {
		try {
			Field field = clazz.getDeclaredField(fieldName);
			field.setAccessible(true);
			return field.get(object);
		}
		catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	static JdbcTable getJdbcTable(RelNode originalRel) {
		return (JdbcTable) get(RelOptTableImpl.class, originalRel.getTable(), "table");
	}

	static String getCatalogName(JdbcTable table) {
		return (String) get(JdbcTable.class, table, "jdbcCatalogName");
	}

	static String getSchemaName(JdbcTable table) {
		return (String) get(JdbcTable.class, table, "jdbcSchemaName");
	}

	static String getTableName(JdbcTable table) {
		// tableName is: [catalog,] [schema,] table
		SqlIdentifier identifier = table.tableName();
		return identifier.names.get(identifier.names.size() - 1);
	}

	static List<String> getQualifiedName(RelOptTable sibling, JdbcTable table) {
		List<String> name = new ArrayList<>();
		if (sibling != null) {
			name.addAll(sibling.getQualifiedName());
			name.remove(name.size() - 1);
		}
		name.add(getTableName(table));
		return name;
	}

	static RelOptTable toRelOptTable(RelOptTable sibling, JdbcTable table) {
		return sibling.getRelOptSchema().getTableForMember(getQualifiedName(sibling, table));
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
		if (!(table instanceof JdbcTable)) {
			throw new UnsupportedOperationException();
		}
		return toRel(cluster, relOptSchema, (JdbcTable) table, qualifiedName);
	}
}
