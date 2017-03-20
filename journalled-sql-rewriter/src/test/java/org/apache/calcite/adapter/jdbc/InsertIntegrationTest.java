package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.runtime.Hook;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class InsertIntegrationTest extends ParameterizedIntegrationBase {
	public InsertIntegrationTest(JournalVersionType versionType) {
		super(versionType, true);
	}

	@Test
	public void testRewritingDeptsWithoutAllColumns() {
		CalciteAssert
				.model(TargetDatabase.makeJournalledModel(versionType))
				.query("INSERT INTO \"" + virtualSchemaName + "\".\"depts\" (\"deptno\") VALUES (696)")
				.withHook(Hook.PROGRAM, JournalledJdbcRuleManager.program())
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcTableModify(table=[[" + virtualSchemaName + ", depts_journal]], operation=[INSERT], flattened=[false])\n" +
						"    JdbcValues(tuples=[[{ 696 }]])\n")
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"depts_journal\" (\"deptno\")\n" +
						"VALUES  (696)", 1);
	}

	@Test
	public void testRewriting() {
		CalciteAssert
				.model(TargetDatabase.makeJournalledModel(versionType))
				.query("INSERT INTO \"" + virtualSchemaName + "\".\"depts\" (\"deptno\", \"department_name\") VALUES (696, 'Pivotal')")
				.withHook(Hook.PROGRAM, JournalledJdbcRuleManager.program())
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcTableModify(table=[[" + virtualSchemaName + ", depts_journal]], operation=[INSERT], flattened=[false])\n" +
						"    JdbcValues(tuples=[[{ 696, 'Pivotal' }]])\n")
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"depts_journal\" (\"deptno\", \"department_name\")\n" +
						"VALUES  (696, 'Pivotal')", 1);
	}

	@Test
	public void testRejectsConflictingID() {
		Assume.assumeTrue("Unique ID enforcement not currently supported for TIMESTAMP", versionType != JournalVersionType.TIMESTAMP);
		try {
			CalciteAssert
					.model(TargetDatabase.makeJournalledModel(versionType))
					.query("INSERT INTO \"" + virtualSchemaName + "\".\"emps\" (\"empid\", \"deptno\", \"last_name\") VALUES (2, 1, 'Me')")
					.withHook(Hook.PROGRAM, JournalledJdbcRuleManager.program())
					.updates(0);
			Assert.fail("Expected duplicate key exception");
		} catch(RuntimeException e) {
			// Look for an error about "duplicate key" and consider it a success. For everything else, it's a fail.
			boolean foundDuplicateError = false;
			Throwable ex = e;
			while (ex != null) {
				if (ex.getMessage().contains("duplicate key")) {
					foundDuplicateError = true;
					break;
				}
				ex = ex.getCause();
			}
			if (!foundDuplicateError) {
				throw e;
			}
		}
	}

	@Test
	public void testAllowsPreviouslyDeletedID() {
		Assume.assumeTrue("Re-use of deleted IDs not currently supported for BIGINT", versionType != JournalVersionType.BIGINT);
		CalciteAssert
				.model(TargetDatabase.makeJournalledModel(versionType))
				.query("INSERT INTO \"" + virtualSchemaName + "\".\"emps\" (\"empid\", \"deptno\", \"last_name\") VALUES (5, 1, 'Me')")
				.withHook(Hook.PROGRAM, JournalledJdbcRuleManager.program())
				.updates(1);
	}

	@Test
	public void testRewritingWithMetaColumnsInTheMiddle() {
		CalciteAssert
				.model(TargetDatabase.makeJournalledModel(versionType))
				.query("INSERT INTO \"" + virtualSchemaName + "\".\"emps\" (\"empid\", \"deptno\", \"first_name\", \"last_name\") VALUES (99, 3, 'Zig', 'Zag')")
				.withHook(Hook.PROGRAM, JournalledJdbcRuleManager.program())
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcTableModify(table=[[" + virtualSchemaName + ", emps_journal]], operation=[INSERT], flattened=[false])\n" +
						"    JdbcValues(tuples=[[{ 99, 3, 'Zig', 'Zag' }]])\n")
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"emps_journal\" (\"empid\", \"deptno\", \"first_name\", \"last_name\")\n" +
						"VALUES  (99, 3, 'Zig', 'Zag')", 1);
	}

	@Test
	public void testRewritingWithoutAllColumns() {
		CalciteAssert
				.model(TargetDatabase.makeJournalledModel(versionType))
				.query("INSERT INTO \"" + virtualSchemaName + "\".\"emps\" (\"empid\", \"deptno\", \"last_name\") VALUES (10, 3, 'OnlyMe')")
				.withHook(Hook.PROGRAM, JournalledJdbcRuleManager.program())
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcTableModify(table=[[" + virtualSchemaName + ", emps_journal]], operation=[INSERT], flattened=[false])\n" +
						"    JdbcValues(tuples=[[{ 10, 3, 'OnlyMe' }]])\n")
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"emps_journal\" (\"empid\", \"deptno\", \"last_name\")\n" +
						"VALUES  (10, 3, 'OnlyMe')", 1);
	}

	@Test
	public void testInsertSelect() {
		CalciteAssert
				.model(TargetDatabase.makeJournalledModel(versionType))
				.query("INSERT INTO \"" + virtualSchemaName + "\".\"emps\" (\"empid\", \"last_name\", \"deptno\") SELECT \"deptno\" + 1000, 'added', \"deptno\" FROM \"" + virtualSchemaName + "\".\"depts\"")
				.withHook(Hook.PROGRAM, JournalledJdbcRuleManager.program())
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcTableModify(table=[[" + virtualSchemaName + ", emps_journal]], operation=[INSERT], flattened=[false])\n" +
						"    JdbcProject(empid=[+($0, 1000)], deptno=[$0], last_name=['added'])\n" +
						"      JdbcFilter(condition=[AND(=($1, $3), IS NULL($2))])\n" +
						"        JdbcProject(deptno=[$0], version_number=[$2], subsequent_version_number=[$3], $f4=[MAX($2) OVER (PARTITION BY $0 ORDER BY $2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)])\n" +
						"          JdbcTableScan(table=[[" + virtualSchemaName + ", depts_journal]])\n")
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"emps_journal\" (\"empid\", \"deptno\", \"last_name\")\n" +
						"(SELECT \"deptno\" + 1000 AS \"empid\", \"deptno\", 'added' AS \"last_name\"\n" +
						"FROM (SELECT \"deptno\", \"version_number\", \"subsequent_version_number\", MAX(\"version_number\") OVER (PARTITION BY \"deptno\" ORDER BY \"version_number\" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS \"$f4\"\n" +
						"FROM \"" + actualSchemaName + "\".\"depts_journal\") AS \"t\"\n" +
						"WHERE \"version_number\" = \"$f4\" AND \"subsequent_version_number\" IS NULL)", 4);
	}

	@Test
	public void testNonJournalled() {
		CalciteAssert
				.model(TargetDatabase.makeJournalledModel(versionType))
				.query("INSERT INTO \"" + virtualSchemaName + "\".\"non_journalled\" (\"id\") VALUES (7)")
				.withHook(Hook.PROGRAM, JournalledJdbcRuleManager.program())
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcTableModify(table=[[" + virtualSchemaName + ", non_journalled]], operation=[INSERT], flattened=[false])\n" +
						"    JdbcValues(tuples=[[{ 7 }]])\n")
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"non_journalled\" (\"id\")\n" +
						"VALUES  (7)", 1);
	}
}
