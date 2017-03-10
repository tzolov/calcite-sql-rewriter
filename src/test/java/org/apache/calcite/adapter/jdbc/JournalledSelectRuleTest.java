package org.apache.calcite.adapter.jdbc;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class JournalledSelectRuleTest {
	private JdbcRelBuilderFactory relBuilderFactory;
	private JdbcRelBuilder relBuilder;
	private JournalledSelectRule rule;
	private JdbcTable journalTable;
	private JdbcTableScan inTableScan;
	private RelOptTable relOptTable;
	private RelOptCluster relOptCluster;
	private RelDataType originalRowType;

	@Before
	@SuppressWarnings("ResultOfMethodCallIgnored") // Mockito syntax
	public void setupMocks() {
		relBuilderFactory = Mockito.mock(JdbcRelBuilderFactory.class);
		relBuilder = Mockito.mock(JdbcRelBuilder.class);
		journalTable = Mockito.mock(JdbcTable.class);
		rule = new JournalledSelectRule();
		relOptTable = Mockito.mock(RelOptTable.class);
		relOptCluster = Mockito.mock(RelOptCluster.class);
		originalRowType = Mockito.mock(RelDataType.class);
		JournalledJdbcTable table = Mockito.mock(JournalledJdbcTable.class);
		RelOptSchema relOptSchema = Mockito.mock(RelOptSchema.class);
		RexInputRef versionField = Mockito.mock(RexInputRef.class);
		RexInputRef subsequentVersionField = Mockito.mock(RexInputRef.class);

		Mockito.doReturn(relBuilder).when(relBuilderFactory).create(Mockito.same(relOptCluster), Mockito.same(relOptSchema));
		Mockito.doReturn(Mockito.mock(RelOptPlanner.class)).when(relOptCluster).getPlanner();
		Mockito.doReturn(relOptSchema).when(relOptTable).getRelOptSchema();
		Mockito.doReturn(ImmutableList.of("theSchema", "theView")).when(relOptTable).getQualifiedName();
		Mockito.doReturn(new SqlIdentifier(
				ImmutableList.of("wrongSchema", "theJournal"),
				null,
				new SqlParserPos(0, 0),
				null
		)).when(journalTable).tableName();
		Mockito.doReturn("myV").when(table).getVersionField();
		Mockito.doReturn("mySV").when(table).getSubsequentVersionField();
		Mockito.doReturn(versionField).when(relBuilder).field(Mockito.eq("myV"));
		Mockito.doReturn(subsequentVersionField).when(relBuilder).field(Mockito.eq("mySV"));
		Mockito.doReturn(journalTable).when(table).getJournalTable();

		inTableScan = new JdbcTableScan(relOptCluster, relOptTable, table, null) {
			@Override
			public RelDataType deriveRowType() {
				return originalRowType;
			}
		};
	}

	@Test
	public void testApply_ignoresNonJdbcTableNodes() {
		RelNode result = rule.apply(Mockito.mock(RelNode.class), relBuilderFactory);
		Assert.assertNull(result);
		Mockito.verify(relBuilderFactory, Mockito.never()).create(Mockito.any(), Mockito.any());
	}

	@Test
	public void testApply_ignoresNonJournalledTables() {
		inTableScan = new JdbcTableScan(relOptCluster, relOptTable, Mockito.mock(JdbcTable.class), null);
		RelNode result = rule.apply(Mockito.mock(RelNode.class), relBuilderFactory);
		Assert.assertNull(result);
		Mockito.verify(relBuilderFactory, Mockito.never()).create(Mockito.any(), Mockito.any());
	}

	@Test
	public void testApply_changesJournalledJdbcTableScans() {
		RelNode builtNode = Mockito.mock(RelNode.class);
		Mockito.doReturn(builtNode).when(relBuilder).build();

		RelNode result = rule.apply(inTableScan, relBuilderFactory);

		Assert.assertEquals(builtNode, result);
	}

	@Test
	public void testApply_changesTableToScanAndCastsResult() {
		rule.apply(inTableScan, relBuilderFactory);

		Mockito.verify(relBuilder).scanJdbc(Mockito.same(journalTable), Mockito.any());
		Mockito.verify(relBuilder).projectToMatch(Mockito.same(originalRowType));
	}
}
