package io.pivotal.beach.calcite.configuration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Schema;

import java.util.*;

public class JournalledTableConfiguration {
	private static Map<Schema, SchemaConfiguration> knownSchemas = new WeakHashMap<>();

	public static void register(Schema schema, Map<String, Object> operand) {
		knownSchemas.put(schema, new SchemaConfiguration(operand));
	}

	public static JournalledTableConfiguration get(Schema schema, String tableName) {
		SchemaConfiguration config = knownSchemas.get(schema);
		if(config == null) {
			return null;
		}
		return config.getTable(tableName);
	}

	private final SchemaConfiguration schemaConfig;
	private final String viewName;
	private final String[]keyColumnNames;

	public String getView() {
		return viewName;
	}

	public List<String> getKeys() {
		return ImmutableList.copyOf(keyColumnNames);
	}

	public String getJournal() {
		return viewName + schemaConfig.journalSuffix;
	}

	public String getVersionField() {
		return schemaConfig.versionField;
	}

	public String getSubsequentVersionField() {
		return schemaConfig.subsequentVersionField;
	}

	private JournalledTableConfiguration(
			SchemaConfiguration schemaConfig,
			String viewName,
			String[] keyColumnNames
	) {
		this.schemaConfig = schemaConfig;
		this.viewName = viewName;
		this.keyColumnNames = keyColumnNames;
	}

	private static class SchemaConfiguration {
		private final ImmutableMap<String,JournalledTableConfiguration> journalledTables;
		private final String journalSuffix;
		private final String versionField;
		private final String subsequentVersionField;

		private SchemaConfiguration(Map<String, Object> operand) {
			Object defaultKeys = operand.get("journalDefaultKey");
			Object tables = operand.get("journalTables");
			if (tables instanceof Map) {
				Map<String,JournalledTableConfiguration> parsedTables = new HashMap<>();
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
					parsedTables.put(name, new JournalledTableConfiguration(this, name, parsedKeys));
				}
				journalledTables = ImmutableMap.copyOf(parsedTables);
			} else {
				journalledTables = ImmutableMap.of();
			}
			journalSuffix = (String) operand.get("journalSuffix");
			versionField = (String) operand.get("journalVersionField");
			subsequentVersionField = (String) operand.get("journalSubsequentVersionField");
		}

		JournalledTableConfiguration getTable(String tableName) {
			return journalledTables.get(tableName);
		}
	}
}
