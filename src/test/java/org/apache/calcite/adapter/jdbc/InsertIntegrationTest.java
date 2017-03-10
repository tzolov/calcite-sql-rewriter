package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.test.CalciteAssert;
import org.junit.BeforeClass;
import org.junit.Test;

public class InsertIntegrationTest {
	private static final String virtualSchemaName = "calcite_sql_rewriter_integration_test"; // Should be "hr" - see TargetDatabase.java
	private static final String actualSchemaName = "calcite_sql_rewriter_integration_test";

	@BeforeClass
	public static void rebuildTestDatabase() throws Exception {
		TargetDatabase.rebuild();
	}

	@Test
	public void testRewriting() {
		CalciteAssert
				.model(TargetDatabase.JOURNALLED_MODEL)
				.query("INSERT INTO \"" + virtualSchemaName + "\".\"depts\" (\"deptno\", \"department_name\") VALUES (696, 'Pivotal')")
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcTableModify(table=[[" + virtualSchemaName + ", depts_journal]], operation=[INSERT], flattened=[false])\n" +
						"    JdbcValues(tuples=[[{ 696, 'Pivotal' }]])\n")
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"depts_journal\" (\"deptno\", \"department_name\")\n" +
						"VALUES  (696, 'Pivotal')", 1);
	}

	@Test
	public void testRewritingWithMetaColumnsInTheMiddle() {
		CalciteAssert
				.model(TargetDatabase.JOURNALLED_MODEL)
				.query("INSERT INTO \"" + virtualSchemaName + "\".\"emps\" (\"empid\", \"deptno\", \"first_name\", \"last_name\") VALUES(99, 3, 'Zig', 'Zag')")
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcTableModify(table=[[" + virtualSchemaName + ", emps_journal]], operation=[INSERT], flattened=[false])\n" +
						"    JdbcValues(tuples=[[{ 99, 3, 'Zig', 'Zag' }]])\n")
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"emps_journal\" (\"empid\", \"deptno\", \"first_name\", \"last_name\")\n" +
						"VALUES  (99, 3, 'Zig', 'Zag')", 1);
	}

	@Test
	public void testRewritingWithoutAllColumns() {
		CalciteAssert
				.model(TargetDatabase.JOURNALLED_MODEL)
				.query("INSERT INTO \"" + virtualSchemaName + "\".\"emps\" (\"empid\", \"deptno\", \"last_name\") VALUES(10, 3, 'OnlyMe')")
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcTableModify(table=[[" + virtualSchemaName + ", emps_journal]], operation=[INSERT], flattened=[false])\n" +
						"    JdbcProject(empid=[$0], deptno=[$1], first_name=[null], last_name=[$2])\n" +
						"      JdbcValues(tuples=[[{ 10, 3, 'OnlyMe' }]])\n");
		// TODO
//				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"emps_journal\" (\"empid\", \"deptno\", \"last_name\")\n" +
//						"VALUES  (99, 3, 'OnlyMe')", 1);
	}

	@Test
	public void testNonJournalled() {
		CalciteAssert
				.model(TargetDatabase.JOURNALLED_MODEL)
				.query("INSERT INTO \"" + virtualSchemaName + "\".\"non_journalled\" (\"id\") VALUES(7)")
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcTableModify(table=[[" + virtualSchemaName + ", non_journalled]], operation=[INSERT], flattened=[false])\n" +
						"    JdbcValues(tuples=[[{ 7 }]])\n")
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"non_journalled\" (\"id\")\n" +
						"VALUES  (7)", 1);
	}
}
