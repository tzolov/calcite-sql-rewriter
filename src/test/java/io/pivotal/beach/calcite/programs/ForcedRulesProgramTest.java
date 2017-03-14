package io.pivotal.beach.calcite.programs;

import java.util.Collections;

import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactory;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactoryFactory;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

public class ForcedRulesProgramTest {
	private Context context;
	private RelOptPlanner planner;
	private RelTraitSet relTraitSet;
	private JdbcRelBuilderFactoryFactory superFactory;
	private JdbcRelBuilderFactory miniFactory;
	private ForcedRule rule;
	private Program program;
	private RelNode inNode;

	@Before
	public void setupMocks() {
		context = Mockito.mock(Context.class);
		planner = Mockito.mock(RelOptPlanner.class);
		relTraitSet = RelTraitSet.createEmpty();
		superFactory = Mockito.mock(JdbcRelBuilderFactoryFactory.class);
		miniFactory = Mockito.mock(JdbcRelBuilderFactory.class);
		rule = Mockito.mock(ForcedRule.class);
		program = new ForcedRulesProgram(superFactory, rule);
		inNode = Mockito.mock(RelNode.class);

		Mockito.doReturn(context).when(planner).getContext();
		Mockito.doReturn(miniFactory).when(superFactory).create(Mockito.same(context));
	}

	@Test
	public void testEmptyProgram_doesNothing() {
		program = new ForcedRulesProgram(superFactory);
		Mockito.doReturn(ImmutableList.of()).when(inNode).getInputs();

		RelNode result = program.run(planner, inNode, relTraitSet, Collections.emptyList(), Collections.emptyList());

		Assert.assertSame(inNode, result);
		Mockito.verify(inNode, Mockito.never()).replaceInput(Mockito.anyInt(), Mockito.any());
	}

	@Test
	public void testRun_sendsRelBuilderFactory() {
		program.run(planner, inNode, relTraitSet, Collections.emptyList(), Collections.emptyList());

		Mockito.verify(rule).apply(Mockito.any(), Mockito.same(miniFactory));
	}

	@Test
	public void testRun_recursesOverTree() {
		RelNode node2a = Mockito.mock(RelNode.class);
		RelNode node2b = Mockito.mock(RelNode.class);
		RelNode node3aa = Mockito.mock(RelNode.class);

		Mockito.doReturn(ImmutableList.of(node2a, node2b)).when(inNode).getInputs();
		Mockito.doReturn(ImmutableList.of(node3aa)).when(node2a).getInputs();
		Mockito.doReturn(ImmutableList.of()).when(node2b).getInputs();
		Mockito.doReturn(ImmutableList.of()).when(node3aa).getInputs();

		RelNode result = program.run(planner, inNode, relTraitSet, Collections.emptyList(), Collections.emptyList());

		Assert.assertSame(inNode, result);
		Mockito.verify(rule).apply(Mockito.same(inNode), Mockito.any());
		Mockito.verify(rule).apply(Mockito.same(node2a), Mockito.any());
		Mockito.verify(rule).apply(Mockito.same(node2b), Mockito.any());
		Mockito.verify(rule).apply(Mockito.same(node3aa), Mockito.any());
	}

	@Test
	public void testRun_returnsReplacementNodes() {
		RelNode outNode = Mockito.mock(RelNode.class);
		Mockito.doReturn(outNode).when(rule).apply(Mockito.same(inNode), Mockito.any());

		RelNode result = program.run(planner, inNode, relTraitSet, Collections.emptyList(), Collections.emptyList());

		Assert.assertSame(outNode, result);
	}

	@Test
	public void testRun_scansWithinReplacements() {
		RelNode outNode = Mockito.mock(RelNode.class);
		Mockito.doReturn(outNode).when(rule).apply(Mockito.same(inNode), Mockito.any());

		program.run(planner, inNode, relTraitSet, Collections.emptyList(), Collections.emptyList());

		Mockito.verify(outNode).getInputs();
	}

	@Test
	public void testRun_replacesChildren() {
		RelNode node2a = Mockito.mock(RelNode.class);
		RelNode node2b = Mockito.mock(RelNode.class);
		RelNode outNode = Mockito.mock(RelNode.class);

		Mockito.doReturn(ImmutableList.of(node2a, node2b)).when(inNode).getInputs();
		Mockito.doReturn(outNode).when(rule).apply(Mockito.same(node2b), Mockito.any());

		RelNode result = program.run(planner, inNode, relTraitSet, Collections.emptyList(), Collections.emptyList());

		Assert.assertSame(inNode, result);
		Mockito.verify(inNode).replaceInput(Mockito.eq(1), Mockito.same(outNode));
	}

	@Test
	public void testRun_onlyAppliesOneRulePerNode() {
		ForcedRule rule2 = Mockito.mock(ForcedRule.class);
		RelNode outNode = Mockito.mock(RelNode.class);
		Mockito.doReturn(outNode).when(rule).apply(Mockito.same(inNode), Mockito.any());

		program = new ForcedRulesProgram(superFactory, rule, rule2);

		program.run(planner, inNode, relTraitSet, Collections.emptyList(), Collections.emptyList());

		Mockito.verify(rule).apply(Mockito.same(inNode), Mockito.any());
		Mockito.verify(rule2, Mockito.never()).apply(Mockito.any(), Mockito.any());
	}
}
