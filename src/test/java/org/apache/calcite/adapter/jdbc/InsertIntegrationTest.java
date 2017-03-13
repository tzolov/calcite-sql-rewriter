package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Program;
import org.apache.calcite.util.Holder;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Function;
import io.pivotal.beach.calcite.programs.ForcedRulesProgram;
import io.pivotal.beach.calcite.programs.SequenceProgram;

public class InsertIntegrationTest {
	private static final String virtualSchemaName = "calcite_sql_rewriter_integration_test"; // Should be "hr" - see TargetDatabase.java
	private static final String actualSchemaName = "calcite_sql_rewriter_integration_test";

	@SuppressWarnings("Guava") // Must conform to Calcite's API
	private Function<Holder<Program>, Void> program = SequenceProgram.prepend(
			new ForcedRulesProgram(new JdbcRelBuilder.FactoryFactory(),
					new JournalledInsertRule(),
					new JournalledUpdateRule(),
					new JournalledDeleteRule()
			)
	);

	@Before // TODO: find out how to make CalciteAssert run in a transaction then change this to BeforeClass
	public void rebuildTestDatabase() throws Exception {
		TargetDatabase.rebuild();
	}

	@Test
	public void testRewritingDeptsWithoutAllColumns() {
		CalciteAssert
				.model(TargetDatabase.JOURNALLED_MODEL)
				.query("INSERT INTO \"" + virtualSchemaName + "\".\"depts\" (\"deptno\") VALUES (696)")
				.withHook(Hook.PROGRAM, program)
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcTableModify(table=[[" + virtualSchemaName + ", depts_journal]], operation=[INSERT], flattened=[false])\n" +
						"    JdbcValues(tuples=[[{ 696 }]])\n")
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"depts_journal\" (\"deptno\")\n" +
						"VALUES  (696)", 1);
	}

	@Test
	public void testRewriting() {
		CalciteAssert
				.model(TargetDatabase.JOURNALLED_MODEL)
				.query("INSERT INTO \"" + virtualSchemaName + "\".\"depts\" (\"deptno\", \"department_name\") VALUES (696, 'Pivotal')")
				.withHook(Hook.PROGRAM, program)
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
				.query("INSERT INTO \"" + virtualSchemaName + "\".\"emps\" (\"empid\", \"deptno\", \"first_name\", \"last_name\") VALUES (99, 3, 'Zig', 'Zag')")
				.withHook(Hook.PROGRAM, program)
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
				.query("INSERT INTO \"" + virtualSchemaName + "\".\"emps\" (\"empid\", \"deptno\", \"last_name\") VALUES (10, 3, 'OnlyMe')")
				.withHook(Hook.PROGRAM, program)
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcTableModify(table=[[" + virtualSchemaName + ", emps_journal]], operation=[INSERT], flattened=[false])\n" +
						"    JdbcValues(tuples=[[{ 10, 3, 'OnlyMe' }]])\n")
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"emps_journal\" (\"empid\", \"deptno\", \"last_name\")\n" +
						"VALUES  (10, 3, 'OnlyMe')", 1);
	}

	@Test
	public void testInsertSelect() {
		CalciteAssert
				.model(TargetDatabase.JOURNALLED_MODEL)
				.query("INSERT INTO \"" + virtualSchemaName + "\".\"emps\" (\"empid\", \"last_name\", \"deptno\") SELECT \"deptno\" + 1000, 'added', \"deptno\" FROM \"" + virtualSchemaName + "\".\"depts\"")
				.withHook(Hook.PROGRAM, program)
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcTableModify(table=[[" + virtualSchemaName + ", emps_journal]], operation=[INSERT], flattened=[false])\n" +
						"    JdbcProject(empid=[+($0, 1000)], deptno=[$0], last_name=['added'])\n" +
						"      JdbcFilter(condition=[AND(=($1, $3), IS NULL($2))])\n" +
						"        JdbcProject(deptno=[$0], version_number=[$2], subsequent_version_number=[$3], $f4=[MAX($2) OVER (PARTITION BY $0 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)])\n" +
						"          JdbcTableScan(table=[[" + virtualSchemaName + ", depts_journal]])\n")
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"emps_journal\" (\"empid\", \"deptno\", \"last_name\")\n" +
						"(SELECT \"deptno\" + 1000 AS \"empid\", \"deptno\", 'added' AS \"last_name\"\n" +
						"FROM (SELECT \"deptno\", \"version_number\", \"subsequent_version_number\", MAX(\"version_number\") OVER (PARTITION BY \"deptno\" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS \"$f4\"\n" +
						"FROM \"" + actualSchemaName + "\".\"depts_journal\") AS \"t\"\n" +
						"WHERE \"version_number\" = \"$f4\" AND \"subsequent_version_number\" IS NULL)", 4);
	}

	@Test
	public void testNonJournalled() {
		CalciteAssert
				.model(TargetDatabase.JOURNALLED_MODEL)
				.query("INSERT INTO \"" + virtualSchemaName + "\".\"non_journalled\" (\"id\") VALUES (7)")
				.withHook(Hook.PROGRAM, program)
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcTableModify(table=[[" + virtualSchemaName + ", non_journalled]], operation=[INSERT], flattened=[false])\n" +
						"    JdbcValues(tuples=[[{ 7 }]])\n")
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"non_journalled\" (\"id\")\n" +
						"VALUES  (7)", 1);
	}
}
