package org.apache.calcite.adapter.jdbc.programs;

import java.util.List;

import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Holder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

@SuppressWarnings("WeakerAccess") // Public API
public class SequenceProgram implements Program {
	private static final Logger logger = LoggerFactory.getLogger(SequenceProgram.class);

	private final ImmutableList<Program> programs;

	public SequenceProgram(Program... programs) {
		this.programs = ImmutableList.copyOf(programs);
	}

	// Use with Hook.PROGRAM.add
	@SuppressWarnings("Guava") // Must conform to Calcite's API
	public static Function<Holder<Program>, Void> prepend(Program program) {
		return (holder) -> {
			if (holder == null) {
				throw new IllegalStateException("No program holder");
			}
			Program chain = holder.get();
			if (chain == null) {
				chain = Programs.standard();
			}
			holder.set(new SequenceProgram(program, chain));
			return null;
		};
	}

	public ImmutableList<Program> getPrograms() {
		return programs;
	}

	public RelNode run(
			RelOptPlanner planner,
			RelNode rel,
			RelTraitSet requiredOutputTraits,
			List<RelOptMaterialization> materializations,
			List<RelOptLattice> lattices
	) {
		for (Program program : programs) {
			rel = program.run(planner, rel, requiredOutputTraits, materializations, lattices);
			logger.debug("After running " + program + ":\n" + RelOptUtil.toString(rel));
		}
		return rel;
	}
}
