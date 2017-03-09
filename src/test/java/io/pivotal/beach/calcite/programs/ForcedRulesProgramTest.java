package io.pivotal.beach.calcite.programs;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.RelBuilderFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class ForcedRulesProgramTest {
	private RelOptPlanner planner;
	private RelTraitSet relTraitSet;
	private BasicForcedRule rule;
	private Program program;
	private RelNode inNode;

	@Before
	public void setupMocks() {
		planner = Mockito.mock(RelOptPlanner.class);
		relTraitSet = RelTraitSet.createEmpty();
		rule = Mockito.mock(BasicForcedRule.class);
		program = new ForcedRulesProgram(rule);
		inNode = Mockito.mock(RelNode.class);
	}

	@Test
	public void testEmptyProgram_doesNothing() {
		program = new ForcedRulesProgram();
		Mockito.doReturn(ImmutableList.of()).when(inNode).getInputs();

		RelNode result = program.run(planner, inNode, relTraitSet, Collections.emptyList(), Collections.emptyList());

		Assert.assertSame(inNode, result);
		Mockito.verify(inNode, Mockito.never()).replaceInput(Mockito.anyInt(), Mockito.any());
	}

	@Test
	public void testRun_sendsRelBuilderFactory() {
		program.run(planner, inNode, relTraitSet, Collections.emptyList(), Collections.emptyList());

		// Requirement of static RelBuilder.proto call means we can't check much about this
		Mockito.verify(rule).apply(Mockito.any(), Mockito.notNull(RelBuilderFactory.class));
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
	public void testRun_doesNotScanWithinReplacements() {
		RelNode outNode = Mockito.mock(RelNode.class);
		Mockito.doReturn(outNode).when(rule).apply(Mockito.same(inNode), Mockito.any());

		program.run(planner, inNode, relTraitSet, Collections.emptyList(), Collections.emptyList());

		Mockito.verify(outNode, Mockito.never()).getInputs();
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
}
