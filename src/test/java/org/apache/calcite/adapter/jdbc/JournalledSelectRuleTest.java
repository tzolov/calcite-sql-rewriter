package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class JournalledSelectRuleTest {
	private RelBuilderFactory relBuilderFactory;
	private JournalledSelectRule rule;
	private RelNode inNode;

	@Before
	public void setupMocks() {
		relBuilderFactory = Mockito.mock(RelBuilderFactory.class);
		rule = new JournalledSelectRule();
		inNode = Mockito.mock(JdbcTableScan.class);
	}

	@Test
	public void testApply_ignoresNonJdbcTableNodes() {
		inNode = Mockito.mock(RelNode.class);
		RelNode result = rule.apply(inNode, relBuilderFactory);
		Assert.assertNull(result);
	}
}
