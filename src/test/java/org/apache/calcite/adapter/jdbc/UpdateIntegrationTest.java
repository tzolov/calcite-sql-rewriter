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

public class UpdateIntegrationTest {
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
	public void testRewriting() {
		CalciteAssert
				.model(TargetDatabase.JOURNALLED_MODEL)
				.query("UPDATE \"" + virtualSchemaName + "\".\"depts\" SET \"department_name\"='First' WHERE \"deptno\" = 3")
				.withHook(Hook.PROGRAM, program)
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcTableModify(table=[[" + virtualSchemaName + ", depts_journal]], operation=[INSERT], flattened=[false])\n" +
						"    JdbcProject(deptno=[$0], department_name=['First'])\n" +
						"      JdbcFilter(condition=[AND(=($1, $3), IS NULL($2), =($0, 3))])\n" +
						"        JdbcProject(deptno=[$0], version_number=[$2], subsequent_version_number=[$3], $f4=[MAX($2) OVER (PARTITION BY $0 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)])\n" +
						"          JdbcTableScan(table=[[" + virtualSchemaName + ", depts_journal]])\n")
				.planUpdateHasSql("INSERT INTO \"" + actualSchemaName + "\".\"depts_journal\" (\"deptno\", \"department_name\")\n" +
						"(SELECT \"deptno\", 'First' AS \"department_name\"\n" +
						"FROM (SELECT \"deptno\", \"version_number\", \"subsequent_version_number\", MAX(\"version_number\") OVER (PARTITION BY \"deptno\" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS \"$f4\"\n" +
						"FROM \"" + actualSchemaName + "\".\"depts_journal\") AS \"t\"\n" +
						"WHERE \"version_number\" = \"$f4\" AND \"subsequent_version_number\" IS NULL AND \"deptno\" = 3)", 1);
	}

	@Test
	public void testNonJournalled() {
		CalciteAssert
				.model(TargetDatabase.JOURNALLED_MODEL)
				.query("UPDATE \"" + virtualSchemaName + "\".\"depts_journal\" SET \"department_name\"='First' WHERE \"deptno\" = 3")
				.withHook(Hook.PROGRAM, program)
				.explainContains("PLAN=JdbcToEnumerableConverter\n" +
						"  JdbcTableModify(table=[[" + virtualSchemaName + ", depts_journal]], operation=[UPDATE], updateColumnList=[[department_name]], sourceExpressionList=[['First']], flattened=[false])\n" +
						"    JdbcProject(deptno=[$0], department_name=[$1], version_number=[$2], subsequent_version_number=[$3], EXPR$0=['First'])\n" +
						"      JdbcFilter(condition=[=($0, 3)])\n" +
						"        JdbcTableScan(table=[[" + virtualSchemaName + ", depts_journal]])\n")
				.planUpdateHasSql("UPDATE \"" + actualSchemaName + "\".\"depts_journal\" SET \"department_name\" = 'First'\nWHERE \"deptno\" = 3", 1);
	}
}
