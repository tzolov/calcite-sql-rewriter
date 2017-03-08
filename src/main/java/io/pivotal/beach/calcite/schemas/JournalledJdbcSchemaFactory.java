package io.pivotal.beach.calcite.schemas;

import io.pivotal.beach.calcite.configuration.JournalledTableConfiguration;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class JournalledJdbcSchemaFactory implements SchemaFactory {
	public static final JournalledJdbcSchemaFactory INSTANCE =
			new JournalledJdbcSchemaFactory(JdbcSchema.Factory.INSTANCE);

	private final SchemaFactory baseFactory;

	private JournalledJdbcSchemaFactory(SchemaFactory base) {
		baseFactory = base;
	}

	public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
		Schema schema = baseFactory.create(parentSchema, name, operand);
		JournalledTableConfiguration.register(schema, operand);
		return schema;
	}
}
