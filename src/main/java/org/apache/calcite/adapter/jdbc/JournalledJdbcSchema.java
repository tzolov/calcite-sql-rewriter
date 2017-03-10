package org.apache.calcite.adapter.jdbc;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import javax.sql.DataSource;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class JournalledJdbcSchema extends JdbcSchema {
	private ImmutableMap<String, JdbcTable> tableMap;
	private final Map<String,String[]> journalledTableKeys;
	private final String journalSuffix;
	private final String versionField;
	private final String subsequentVersionField;

	private JournalledJdbcSchema(
			DataSource dataSource,
			SqlDialect dialect,
			JdbcConvention convention,
			String catalog,
			String schema,
			Map<String, Object> operand
	) {
		super(dataSource, dialect, convention, catalog, schema);

		tableMap = null;
		journalledTableKeys = new HashMap<>();
		journalSuffix = (String) operand.get("journalSuffix");
		versionField = (String) operand.get("journalVersionField");
		subsequentVersionField = (String) operand.get("journalSubsequentVersionField");

		Object defaultKeys = operand.get("journalDefaultKey");
		Object tables = operand.get("journalTables");
		if (tables instanceof Map) {
			for (Map.Entry<?,?> entry : ((Map<?,?>) tables).entrySet()) {
				String name = (String) entry.getKey();
				Object keys = entry.getValue();
				if(keys == null) {
					keys = defaultKeys;
				}
				String[] parsedKeys;
				if(keys instanceof String) {
					parsedKeys = new String[]{(String) keys};
				} else if(keys instanceof Collection) {
					parsedKeys = new String[((Collection<?>) keys).size()];
					int i = 0;
					for(Object key : (Collection<?>) keys) {
						parsedKeys[i] = (String) key;
						i ++;
					}
				} else {
					throw new IllegalArgumentException("No primary key given for table: " + name);
				}
				journalledTableKeys.put(name, parsedKeys);
			}
		}
	}

	public String getVersionField() {
		return versionField;
	}

	public String getSubsequentVersionField() {
		return subsequentVersionField;
	}

	// Copied from JdbcSchema with modifications
	public static JournalledJdbcSchema create(
			SchemaPlus parentSchema,
			String name,
			Map<String, Object> operand
	) {
		DataSource dataSource;
		try {
			final String dataSourceName = (String) operand.get("dataSource");
			if (dataSourceName != null) {
				dataSource = AvaticaUtils.instantiatePlugin(DataSource.class, dataSourceName);
			} else {
				final String jdbcUrl = (String) operand.get("jdbcUrl");
				final String jdbcDriver = (String) operand.get("jdbcDriver");
				final String jdbcUser = (String) operand.get("jdbcUser");
				final String jdbcPassword = (String) operand.get("jdbcPassword");
				dataSource = dataSource(jdbcUrl, jdbcDriver, jdbcUser, jdbcPassword);
			}
		} catch (Exception e) {
			throw new RuntimeException("Error while reading dataSource", e);
		}
		String catalog = (String) operand.get("jdbcCatalog");
		String schema = (String) operand.get("jdbcSchema");
		final Expression expression = Schemas.subSchemaExpression(parentSchema, name, JdbcSchema.class);
		final SqlDialect dialect = createDialect(dataSource);
		final JdbcConvention convention = JdbcConvention.of(dialect, expression, name);
		return new JournalledJdbcSchema(dataSource, dialect, convention, catalog, schema, operand);
	}

	@Override
	public Table getTable(String name) {
		return getTableMap(false).get(name);
	}

	@Override
	public Set<String> getTableNames() {
		// This method is called during a cache refresh. We can take it as a signal
		// that we need to re-build our own cache.
		return getTableMap(true).keySet();
	}

	@Override
	RelProtoDataType getRelDataType(
			DatabaseMetaData metaData,
			String catalogName,
			String schemaName,
			String tableName
	) throws SQLException {
		if(journalledTableKeys.containsKey(tableName)) {
			// 1: Find columns for journal table
			RelDataType relDataType = super
					.getRelDataType(metaData, catalogName, schemaName, journalNameFor(tableName))
					.apply(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT) {
						@Override
						public RelDataType copyType(RelDataType type) {
							return type;
						}
					});

			RelDataTypeFactory.FieldInfoBuilder fieldInfo = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT).builder();

			// 2: Filter out journal-implementation columns
			for(RelDataTypeField field : relDataType.getFieldList()) {
				String fieldName = field.getName();
				if(fieldName.equals(versionField) || fieldName.equals(subsequentVersionField)) {
					continue;
				}
				fieldInfo.add(field);
			}

			return RelDataTypeImpl.proto(fieldInfo.build());
		} else {
			return super.getRelDataType(metaData, catalogName, schemaName, tableName);
		}
	}

	// Copied from JdbcSchema
	private synchronized Map<String, JdbcTable> getTableMap(boolean force) {
		if (force || tableMap == null) {
			tableMap = ImmutableMap.copyOf(computeTables());
		}
		return tableMap;
	}

	private Map<String, JdbcTable> computeTables() {
		// 1: Get all tables from the DB as usual
		Set<String> rawTableNames = super.getTableNames(); // Forces computeTables
		Map<String, JdbcTable> tables = new HashMap<>();
		for(String rawTableName : rawTableNames) {
			tables.put(rawTableName, (JdbcTable) super.getTable(rawTableName));
		}

		// 2: Filter out any table/view which has a name in the journalled list
		tables.keySet().removeAll(journalledTableKeys.keySet());

		// 3: For each table in the journalled list, generate a fake table from its journal
		for(String virtualName : journalledTableKeys.keySet()) {
			JdbcTable journalTable = tables.get(journalNameFor(virtualName));
			if(journalTable != null) {
				tables.put(virtualName, new JournalledJdbcTable(
						virtualName,
						this,
						journalTable,
						journalledTableKeys.get(virtualName)
				));
			}
		}

		return tables;
	}

	private String journalNameFor(String virtualName) {
		return virtualName + journalSuffix;
	}

	@SuppressWarnings("unused") // Used by .json configuration
	public static class Factory implements SchemaFactory {
		public static final Factory INSTANCE = new Factory();

		private Factory() {
		}

		public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
			return JournalledJdbcSchema.create(parentSchema, name, operand);
		}
	}
}
