package org.apache.calcite.adapter.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.jdbc.programs.ForcedRulesProgram;
import org.apache.calcite.adapter.jdbc.programs.SequenceProgram;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.Program;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class JournalledJdbcSchemaTest {
	private static final String DBURL = "jdbc:hsqldb:mem:basic";

	private Map<String, Object> options;

	@BeforeClass
	public static void configureInMemoryDB() throws Exception {
		Class.forName(org.hsqldb.jdbc.JDBCDriver.class.getName());
		Connection connection = DriverManager.getConnection(DBURL);
		Statement statement = connection.createStatement();
		statement.execute("CREATE TABLE MY_TABLE_J (id INT NOT NULL, myvfield TIMESTAMP, mysvfield TIMESTAMP)");
		statement.execute("CREATE TABLE OTHER_TABLE (id INT NOT NULL, myvfield TIMESTAMP, mysvfield TIMESTAMP)");
		statement.close();
		connection.close();
	}

	@Before
	public void prepare() {
		JournalledJdbcSchema.Factory.INSTANCE.setAutomaticallyAddRules(false);

		options = new HashMap<>();
		options.put("jdbcDriver", org.hsqldb.jdbc.JDBCDriver.class.getName());
		options.put("jdbcUrl", DBURL);
		options.put("journalVersionField", "myvfield");
		options.put("journalSubsequentVersionField", "mysvfield");
		options.put("journalSuffix", "_J");
		options.put("journalDefaultKey", ImmutableList.of("def1", "def2"));
		options.put("journalTables", ImmutableMap.of("MY_TABLE", ImmutableList.of("myKey1", "myKey2")));
	}

	@Test
	public void testFactoryCreatesJournalledJdbcSchema() {
		Schema schema = JournalledJdbcSchema.Factory.INSTANCE.create(null, "my-parent", options);
		Assert.assertTrue(schema instanceof JournalledJdbcSchema);

		JournalledJdbcSchema journalledSchema = (JournalledJdbcSchema) schema;

		Assert.assertEquals(journalledSchema.getVersionField(), "myvfield");
		Assert.assertEquals(journalledSchema.getSubsequentVersionField(), "mysvfield");
	}

	@Test
	public void testFactoryWillAutomaticallyAddRules() {
		// This test changes the global state of Calcite! It shouldn't cause issues elsewhere since the rules avoid
		// changing unrecognised tables, so will not apply to their own output.
		JournalledJdbcSchema.Factory.INSTANCE.setAutomaticallyAddRules(true);
		JournalledJdbcSchema.Factory.INSTANCE.create(null, "my-parent", options);
		try {
			Program def = Mockito.mock(Program.class);
			Holder<Program> holder = Holder.of(def);
			Hook.PROGRAM.run(Pair.of(null, holder));
			Assert.assertTrue(holder.get() instanceof SequenceProgram);
			Assert.assertTrue(((SequenceProgram) holder.get()).getPrograms().get(0) instanceof ForcedRulesProgram);
		} finally {
			// ensure no gap where another hook may be added
			JournalledJdbcSchema.Factory.INSTANCE.setAutomaticallyAddRules(false);
			JournalledJdbcRuleManager.removeHook();
		}
	}

	private JournalledJdbcSchema makeSchema() {
		return (JournalledJdbcSchema) JournalledJdbcSchema.Factory.INSTANCE.create(null, "my-parent", options);
	}

	@Test
	public void testCanLoadConnectionDetailsFromExternalFile() {
		options.remove("jdbcDriver");
		options.remove("jdbcUrl");
		options.put("connection", "connection.json");
		makeSchema();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFactoryRejectsAbsentJournalTables() {
		options.remove("journalTables");
		makeSchema();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFactoryRejectsInvalidConnections() {
		options.put("connection", "missingFile.json");
		makeSchema();
	}

	@Test
	public void testDefaultVersionField() {
		options.remove("journalVersionField");
		JournalledJdbcSchema schema = makeSchema();
		Assert.assertEquals(schema.getVersionField(), "version_number");
	}

	@Test
	public void testDefaultSubsequentVersionField() {
		options.remove("journalSubsequentVersionField");
		JournalledJdbcSchema schema = makeSchema();
		Assert.assertEquals(schema.getSubsequentVersionField(), "subsequent_version_number");
	}

	@Test
	public void testDefaultJournalSuffix() {
		options.remove("journalSuffix");
		JournalledJdbcSchema schema = makeSchema();
		Assert.assertEquals(schema.journalNameFor("foo"), "foo_journal");
	}

	@Test
	public void testDefaultsToTimestampVersioning() {
		JournalledJdbcSchema schema = makeSchema();
		Assert.assertEquals(schema.getVersionType(), JournalVersionType.TIMESTAMP);
	}

	@Test
	public void testVersionTypeCanBeChanged() {
		options.put("journalVersionType", "BIGINT");
		JournalledJdbcSchema schema = makeSchema();
		Assert.assertEquals(schema.getVersionType(), JournalVersionType.BIGINT);
	}

	@Test
	public void testVersionTypeIsCaseInsensitive() {
		options.put("journalVersionType", "bigint");
		JournalledJdbcSchema schema = makeSchema();
		Assert.assertEquals(schema.getVersionType(), JournalVersionType.BIGINT);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testVersionTypeMustBeKnown() {
		options.put("journalVersionType", "nope");
		makeSchema();
	}

	@Test
	public void testGetTableNamesReturnsVirtualTables() {
		JournalledJdbcSchema schema = makeSchema();
		Set<String> names = schema.getTableNames();
		Assert.assertTrue(names.contains("MY_TABLE"));
		Assert.assertTrue(names.contains("OTHER_TABLE"));
	}

	@Test
	public void testGetTableConvertsMatchingTables() {
		JournalledJdbcSchema schema = makeSchema();
		Table table = schema.getTable("MY_TABLE");
		Assert.assertNotNull(table);
		Assert.assertTrue(table instanceof JournalledJdbcTable);
		JournalledJdbcTable journalTable = (JournalledJdbcTable) table;
		Assert.assertEquals(journalTable.getVersionField(), "myvfield");
		Assert.assertEquals(journalTable.getSubsequentVersionField(), "mysvfield");
		Assert.assertNotNull(journalTable.getJournalTable());
	}

	@Test
	public void testGetTablePassesThroughNonMatchingTables() {
		JournalledJdbcSchema schema = makeSchema();
		Table table = schema.getTable("OTHER_TABLE");
		Assert.assertNotNull(table);
		Assert.assertFalse(table instanceof JournalledJdbcTable);
	}

	@Test
	public void testGetTableSurvivesNulls() {
		JournalledJdbcSchema schema = makeSchema();
		Table table = schema.getTable("NOT_HERE");
		Assert.assertNull(table);
	}

	@Test
	public void testListsOfKeysAreLoaded() {
		JournalledJdbcSchema schema = makeSchema();
		JournalledJdbcTable journalTable = (JournalledJdbcTable) schema.getTable("MY_TABLE");
		Assert.assertEquals(journalTable.getKeyColumnNames(), ImmutableList.of("myKey1", "myKey2"));
	}

	@Test
	public void testSingleStringKeysAreLoaded() {
		options.put("journalTables", ImmutableMap.of("MY_TABLE", "foo"));
		JournalledJdbcSchema schema = makeSchema();
		JournalledJdbcTable journalTable = (JournalledJdbcTable) schema.getTable("MY_TABLE");
		Assert.assertEquals(journalTable.getKeyColumnNames(), ImmutableList.of("foo"));
	}

	@Test
	public void testDefaultKeysAreLoadedIfNoSpecificKeysGiven() {
		options.put("journalTables", Collections.singletonMap("MY_TABLE", null));
		JournalledJdbcSchema schema = makeSchema();
		JournalledJdbcTable journalTable = (JournalledJdbcTable) schema.getTable("MY_TABLE");
		Assert.assertEquals(journalTable.getKeyColumnNames(), ImmutableList.of("def1", "def2"));
	}

	@Test
	public void testListsOfTablesAreGivenDefaultKeys() {
		options.put("journalTables", ImmutableList.of("MY_TABLE"));
		JournalledJdbcSchema schema = makeSchema();
		JournalledJdbcTable journalTable = (JournalledJdbcTable) schema.getTable("MY_TABLE");
		Assert.assertEquals(journalTable.getKeyColumnNames(), ImmutableList.of("def1", "def2"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMissingKeysAreRejected() {
		options.remove("journalDefaultKey");
		options.put("journalTables", Collections.singletonMap("MY_TABLE", null));
		makeSchema();
	}
}
