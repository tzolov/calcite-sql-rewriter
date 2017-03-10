package org.apache.calcite.adapter.jdbc;

import com.google.common.base.Function;
import io.pivotal.beach.calcite.programs.ForcedRulesProgram;
import io.pivotal.beach.calcite.programs.SequenceProgram;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Program;
import org.apache.calcite.util.Holder;
import org.junit.BeforeClass;
import org.junit.Test;

public class SelectIntegrationTest {
	private static final String virtualSchemaName = "hr";
	private static final String actualSchemaName = "calcite_sql_rewriter_integration_test";

	@SuppressWarnings("Guava") // Must conform to Calcite's API
	private Function<Holder<Program>, Void> program = SequenceProgram.prepend(
			new ForcedRulesProgram(new JdbcRelBuilder.FactoryFactory(),
					new JournalledSelectRule()
			)
	);

	@BeforeClass
	public static void rebuildTestDatabase() throws Exception {
		TargetDatabase.rebuild();
	}

	@Test
	public void testRewriting() {
		CalciteAssert
				.model(TargetDatabase.JOURNALLED_MODEL)
				.query("SELECT \"empid\" FROM \"" + virtualSchemaName + "\".\"emps\"")
				.withHook(Hook.PROGRAM, program)
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcProject(empid=[$0])\n" +
						"    JdbcFilter(condition=[AND(=($1, $3), IS NULL($2))])\n" +
						"      JdbcProject(empid=[$0], version_number=[$2], subsequent_version_number=[$3], $f6=[MAX($2) OVER (PARTITION BY $0 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)])\n" +
						"        JdbcTableScan(table=[[" + virtualSchemaName + ", emps_journal]])\n")
				.runs()
				.planHasSql("SELECT \"empid\"\n" +
						"FROM (SELECT \"empid\", \"version_number\", \"subsequent_version_number\", MAX(\"version_number\") OVER (PARTITION BY \"empid\" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS \"$f6\"\n" +
						"FROM \"" + actualSchemaName + "\".\"emps_journal\") AS \"t\"\n" +
						"WHERE \"version_number\" = \"$f6\" AND \"subsequent_version_number\" IS NULL")
				.returns("empid=1\n" +
						"empid=2\n" +
						"empid=3\n" +
						"empid=4\n" +
						"empid=6\n");
	}

	@Test
	public void testComplexRewriting() {
		CalciteAssert
				.model(TargetDatabase.JOURNALLED_MODEL)
				.query("SELECT \"d\".\"deptno\"\n" +
						"FROM \"" + virtualSchemaName + "\".\"emps\" AS \"e\"\n" +
						"JOIN \"" + virtualSchemaName + "\".\"depts\" AS \"d\" ON \"e\".\"deptno\" = \"d\".\"deptno\"\n" +
						"GROUP BY \"d\".\"deptno\"\n" +
						"HAVING COUNT(*) > 1")
				.withHook(Hook.PROGRAM, program)
				.runs()
				.returns("deptno=2\n");
	}

	@Test
	public void testNonJournalled() {
		CalciteAssert
				.model(TargetDatabase.JOURNALLED_MODEL)
				.query("SELECT * FROM \"" + virtualSchemaName + "\".\"non_journalled\"")
				.withHook(Hook.PROGRAM, program)
				.explainContains("JdbcToEnumerableConverter\n" +
						"  JdbcTableScan(table=[[" + virtualSchemaName + ", non_journalled]])\n")
				.runs()
				.planHasSql("SELECT *\n" +
						"FROM \"" + actualSchemaName + "\".\"non_journalled\"")
				.returns("id=1\n" +
						"id=2\n");
	}

}
