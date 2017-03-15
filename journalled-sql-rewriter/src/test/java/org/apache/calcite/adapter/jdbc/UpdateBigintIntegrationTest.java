package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.runtime.Hook;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Before;
import org.junit.Test;

public class UpdateBigintIntegrationTest {
	private static final String virtualSchemaName = "calcite_sql_rewriter_integration_test"; // Should be "hr" - see TargetDatabase.java
	private static final String actualSchemaName = "calcite_sql_rewriter_integration_test";
	private final JournalVersionType versionType = JournalVersionType.BIGINT;

	@Before // TODO: find out how to make CalciteAssert run in a transaction then change this to BeforeClass
	public void rebuildTestDatabase() throws Exception {
		TargetDatabase.rebuild(versionType);
		JournalledJdbcSchema.Factory.INSTANCE.setAutomaticallyAddRules(false);
	}

	@Test
	public void testRewriting() {
		CalciteAssert
				.model(TargetDatabase.makeJournalledModel(versionType))
				.query("UPDATE \"" + virtualSchemaName + "\".\"depts\" SET \"department_name\"='First' WHERE \"deptno\" = 3")
				.withHook(Hook.PROGRAM, JournalledJdbcRuleManager.program())
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"depts_journal\" (\"deptno\", \"department_name\", \"version_number\")\n" +
						"(SELECT \"deptno\", 'First' AS \"department_name\", \"version_number\" + 1 AS \"version_number\"\n" +
						"FROM (SELECT \"deptno\", \"version_number\", \"subsequent_version_number\", MAX(\"version_number\") OVER (PARTITION BY \"deptno\" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS \"$f4\"\n" +
						"FROM \"" + actualSchemaName + "\".\"depts_journal\") AS \"t\"\n" +
						"WHERE \"version_number\" = \"$f4\" AND \"subsequent_version_number\" IS NULL AND \"deptno\" = 3)", 1);
	}
}
