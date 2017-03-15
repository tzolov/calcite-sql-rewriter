package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.runtime.Hook;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class DeleteTimestampIntegrationTest extends IntegrationBase {
	public DeleteTimestampIntegrationTest() {
		super(JournalVersionType.TIMESTAMP, true);
	}

	@Test
	public void testRewriting() {
		CalciteAssert
				.model(TargetDatabase.makeJournalledModel(versionType))
				.query("DELETE FROM \"" + virtualSchemaName + "\".\"depts\" WHERE \"deptno\"=3")
				.withHook(Hook.PROGRAM, JournalledJdbcRuleManager.program())
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcTableModify(table=[[" + virtualSchemaName + ", depts_journal]], operation=[INSERT], flattened=[false])\n" +
						"    JdbcProject(deptno=[$0], department_name=[$1], version_number=[CURRENT_TIMESTAMP], subsequent_version_number=[CURRENT_TIMESTAMP])\n" +
						"      JdbcFilter(condition=[AND(=($2, $4), IS NULL($3), =($0, 3))])\n" +
						"        JdbcProject(deptno=[$0], department_name=[$1], version_number=[$2], subsequent_version_number=[$3], $f4=[MAX($2) OVER (PARTITION BY $0 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)])\n" +
						"          JdbcTableScan(table=[[" + virtualSchemaName + ", depts_journal]])\n")
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"depts_journal\" (\"deptno\", \"department_name\", \"version_number\", \"subsequent_version_number\")\n" +
						"(SELECT \"deptno\", \"department_name\", CURRENT_TIMESTAMP AS \"version_number\", CURRENT_TIMESTAMP AS \"subsequent_version_number\"\n" +
						"FROM (SELECT \"deptno\", \"department_name\", \"version_number\", \"subsequent_version_number\", MAX(\"version_number\") OVER (PARTITION BY \"deptno\" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS \"$f4\"\n" +
						"FROM \"" + actualSchemaName + "\".\"depts_journal\") AS \"t\"\n" +
						"WHERE \"version_number\" = \"$f4\" AND \"subsequent_version_number\" IS NULL AND \"deptno\" = 3)", 1);
	}

	@Test
	public void testDeletingAbsentRecord() {
		CalciteAssert
				.model(TargetDatabase.makeJournalledModel(versionType))
				.query("DELETE FROM \"" + virtualSchemaName + "\".\"depts\" WHERE \"deptno\"=999")
				.withHook(Hook.PROGRAM, JournalledJdbcRuleManager.program())
				.updates(0);
	}

	@Test
	public void testDeletingByNonKeyColumns() {
		CalciteAssert
				.model(TargetDatabase.makeJournalledModel(versionType))
				.query("DELETE FROM \"" + virtualSchemaName + "\".\"depts\" WHERE \"department_name\"='Dep3'")
				.withHook(Hook.PROGRAM, JournalledJdbcRuleManager.program())
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"depts_journal\" (\"deptno\", \"department_name\", \"version_number\", \"subsequent_version_number\")\n" +
						"(SELECT \"deptno\", \"department_name\", CURRENT_TIMESTAMP AS \"version_number\", CURRENT_TIMESTAMP AS \"subsequent_version_number\"\n" +
						"FROM (SELECT \"deptno\", \"department_name\", \"version_number\", \"subsequent_version_number\", MAX(\"version_number\") OVER (PARTITION BY \"deptno\" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS \"$f4\"\n" +
						"FROM \"" + actualSchemaName + "\".\"depts_journal\") AS \"t\"\n" +
						"WHERE \"version_number\" = \"$f4\" AND \"subsequent_version_number\" IS NULL AND \"department_name\" = 'Dep3')", 1);
	}

}
