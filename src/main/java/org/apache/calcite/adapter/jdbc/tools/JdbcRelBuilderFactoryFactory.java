package org.apache.calcite.adapter.jdbc.tools;

import org.apache.calcite.plan.Context;

public interface JdbcRelBuilderFactoryFactory {
	JdbcRelBuilderFactory create(Context context);
}
