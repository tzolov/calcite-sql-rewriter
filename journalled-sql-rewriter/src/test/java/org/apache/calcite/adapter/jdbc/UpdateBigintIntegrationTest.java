package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.runtime.Hook;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class UpdateBigintIntegrationTest extends IntegrationBase {
	public UpdateBigintIntegrationTest() {
		super(JournalVersionType.BIGINT, true);
	}

	@Test
	public void testRewriting() {
		CalciteAssert
				.model(TargetDatabase.makeJournalledModel(versionType))
				.query("UPDATE \"" + virtualSchemaName + "\".\"depts\" SET \"department_name\"='First' WHERE \"deptno\" = 3")
				.withHook(Hook.PROGRAM, JournalledJdbcRuleManager.program())
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"depts_journal\" (\"deptno\", \"department_name\", \"version_number\")\n" +
						"(SELECT \"deptno\", 'First' AS \"department_name\", \"version_number\" + 1 AS \"version_number\"\n" +
						"FROM (SELECT \"deptno\", \"version_number\", \"subsequent_version_number\", MAX(\"version_number\") OVER (PARTITION BY \"deptno\" ORDER BY \"version_number\" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS \"$f4\"\n" +
						"FROM \"" + actualSchemaName + "\".\"depts_journal\") AS \"t\"\n" +
						"WHERE \"version_number\" = \"$f4\" AND \"subsequent_version_number\" IS NULL AND \"deptno\" = 3)", 1);
	}

	@Test(expected = RuntimeException.class)
	public void testRewritingWithIncorrectVersionType() {
		String model = TargetDatabase.makeJournalledModel(versionType);
		model = model.replace("BIGINT", "TIMESTAMP");
		CalciteAssert
				.model(model)
				.query("UPDATE \"" + virtualSchemaName + "\".\"depts\" SET \"department_name\"='First' WHERE \"deptno\" = 3")
				.withHook(Hook.PROGRAM, JournalledJdbcRuleManager.program())
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"depts_journal\" (\"deptno\", \"department_name\", \"version_number\")\n" +
						"(SELECT \"deptno\", 'First' AS \"department_name\", \"version_number\" + 1 AS \"version_number\"\n" +
						"FROM (SELECT \"deptno\", \"version_number\", \"subsequent_version_number\", MAX(\"version_number\") OVER (PARTITION BY \"deptno\" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS \"$f4\"\n" +
						"FROM \"" + actualSchemaName + "\".\"depts_journal\") AS \"t\"\n" +
						"WHERE \"version_number\" = \"$f4\" AND \"subsequent_version_number\" IS NULL AND \"deptno\" = 3)", 1);
	}
}
