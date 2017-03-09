package io.pivotal.beach.calcite.programs;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;
import org.apache.calcite.util.Holder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class SequenceProgramTest {
	private RelOptPlanner planner;
	private RelTraitSet relTraitSet;
	private List<RelOptMaterialization> materializations;
	private List<RelOptLattice> lattices;
	private Program program;
	private Program subProgram;
	private RelNode inNode;

	@Before
	public void setupMocks() {
		planner = Mockito.mock(RelOptPlanner.class);
		relTraitSet = RelTraitSet.createEmpty();
		materializations = new ArrayList<>();
		lattices = new ArrayList<>();
		subProgram = Mockito.mock(Program.class);
		program = new SequenceProgram(subProgram);
		inNode = Mockito.mock(RelNode.class);
	}

	@Test
	public void testEmptyProgram_doesNothing() {
		program = new SequenceProgram();

		RelNode result = program.run(planner, inNode, relTraitSet, materializations, lattices);

		Assert.assertSame(inNode, result);
	}

	@Test
	public void testRun_propagatesToSubProgram() {
		RelNode node2 = Mockito.mock(RelNode.class);
		Mockito.doReturn(node2).when(subProgram).run(Mockito.any(), Mockito.same(inNode), Mockito.any(), Mockito.any(), Mockito.any());

		RelNode result = program.run(planner, inNode, relTraitSet, materializations, lattices);

		Assert.assertSame(node2, result);
	}

	@Test
	public void testRun_propagatesAllParameters() {
		RelNode node2 = Mockito.mock(RelNode.class);
		Mockito.doReturn(node2).when(subProgram).run(Mockito.any(), Mockito.same(inNode), Mockito.any(), Mockito.any(), Mockito.any());

		program.run(planner, inNode, relTraitSet, materializations, lattices);

		Mockito.verify(subProgram).run(
				Mockito.same(planner),
				Mockito.any(),
				Mockito.same(relTraitSet),
				Mockito.same(materializations),
				Mockito.same(lattices)
		);
	}

	@Test
	public void testRun_chainsSubPrograms() {
		Program subProgram2 = Mockito.mock(Program.class);
		RelNode node2 = Mockito.mock(RelNode.class);
		RelNode node3 = Mockito.mock(RelNode.class);
		program = new SequenceProgram(subProgram, subProgram2);
		Mockito.doReturn(node2).when(subProgram).run(Mockito.any(), Mockito.same(inNode), Mockito.any(), Mockito.any(), Mockito.any());
		Mockito.doReturn(node3).when(subProgram2).run(Mockito.any(), Mockito.same(node2), Mockito.any(), Mockito.any(), Mockito.any());

		RelNode result = program.run(planner, inNode, relTraitSet, materializations, lattices);

		Assert.assertSame(node3, result);
	}

	@Test
	public void testPrepend_returnsAHookForAddingTheProgram() {
		Program originalProgram = Mockito.mock(Program.class);
		Holder<Program> holder = Holder.of(originalProgram);

		SequenceProgram.prepend(subProgram).apply(holder);

		Program newProgram = holder.get();
		Assert.assertTrue(newProgram instanceof SequenceProgram);
		ImmutableList<Program> subPrograms = ((SequenceProgram) newProgram).getPrograms();
		Assert.assertEquals(2, subPrograms.size());
		Assert.assertSame(subProgram, subPrograms.get(0));
		Assert.assertSame(originalProgram, subPrograms.get(1));
	}

	@Test
	public void testPrepend_usesTheDefaultProgramIfGivenNull() {
		Holder<Program> holder = Holder.of(null);

		SequenceProgram.prepend(subProgram).apply(holder);

		Program newProgram = holder.get();
		Assert.assertTrue(newProgram instanceof SequenceProgram);
		ImmutableList<Program> subPrograms = ((SequenceProgram) newProgram).getPrograms();
		// Not much we can check about the program; it is built inline and has no particular features
		Assert.assertNotNull(subPrograms.get(1));
	}

	@Test(expected = RuntimeException.class)
	public void testPrepend_throwsIfNoHolderIsGiven() {
		SequenceProgram.prepend(subProgram).apply(null);
	}
}
