package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.runtime.Hook;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class SelectIntegrationTest extends ParameterizedIntegrationBase {
	public SelectIntegrationTest(JournalVersionType versionType) {
		super(versionType, false);
	}

	@Test
	public void testRewriting() {
		CalciteAssert
				.model(TargetDatabase.makeJournalledModel(versionType))
				.query("SELECT \"empid\" FROM \"" + virtualSchemaName + "\".\"emps\"")
				.withHook(Hook.PROGRAM, JournalledJdbcRuleManager.program())
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcProject(empid=[$0])\n" +
						"    JdbcFilter(condition=[AND(=($1, $3), IS NULL($2))])\n" +
						"      JdbcProject(empid=[$0], version_number=[$2], subsequent_version_number=[$3], $f6=[MAX($2) OVER (PARTITION BY $0 ORDER BY $2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)])\n" +
						"        JdbcTableScan(table=[[" + virtualSchemaName + ", emps_journal]])\n")
				.planHasSql("SELECT \"empid\"\n" +
						"FROM (SELECT \"empid\", \"version_number\", \"subsequent_version_number\", MAX(\"version_number\") OVER (PARTITION BY \"empid\" ORDER BY \"version_number\" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS \"$f6\"\n" +
						"FROM \"" + actualSchemaName + "\".\"emps_journal\") AS \"t\"\n" +
						"WHERE \"version_number\" = \"$f6\" AND \"subsequent_version_number\" IS NULL")
				.returns("empid=1\n" +
						"empid=2\n" +
						"empid=3\n" +
						"empid=4\n" +
						"empid=6\n");
	}

	@Test
	public void testAllColumns() {
		CalciteAssert
				.model(TargetDatabase.makeJournalledModel(versionType))
				.query("SELECT * FROM \"" + virtualSchemaName + "\".\"emps\"")
				.withHook(Hook.PROGRAM, JournalledJdbcRuleManager.program())
				.returns("empid=1; deptno=2; first_name=Peter; last_name=Pan\n" +
						"empid=2; deptno=1; first_name=Ian; last_name=Bibian\n" +
						"empid=3; deptno=2; first_name=Victor; last_name=Strugatski\n" +
						"empid=4; deptno=2; first_name=Stan; last_name=Ban\n" +
						"empid=6; deptno=4; first_name=Ivan; last_name=Baraban\n");
	}

	@Test
	public void testComplexRewriting() {
		CalciteAssert
				.model(TargetDatabase.makeJournalledModel(versionType))
				.query("SELECT \"d\".\"deptno\"\n" +
						"FROM \"" + virtualSchemaName + "\".\"emps\" AS \"e\"\n" +
						"JOIN \"" + virtualSchemaName + "\".\"depts\" AS \"d\" ON \"e\".\"deptno\" = \"d\".\"deptno\"\n" +
						"GROUP BY \"d\".\"deptno\"\n" +
						"HAVING COUNT(*) > 1")
				.withHook(Hook.PROGRAM, JournalledJdbcRuleManager.program())
				.returns("deptno=2\n");
	}

	@Test
	public void testNonJournalled() {
		CalciteAssert
				.model(TargetDatabase.makeJournalledModel(versionType))
				.query("SELECT * FROM \"" + virtualSchemaName + "\".\"non_journalled\"")
				.withHook(Hook.PROGRAM, JournalledJdbcRuleManager.program())
				.explainContains("JdbcToEnumerableConverter\n" +
						"  JdbcTableScan(table=[[" + virtualSchemaName + ", non_journalled]])\n")
				.planHasSql("SELECT *\n" +
						"FROM \"" + actualSchemaName + "\".\"non_journalled\"")
				.returns("id=1\n" +
						"id=2\n");
	}
}
