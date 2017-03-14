package org.apache.calcite.adapter.jdbc.programs;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Pair;
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
	public static Function<Pair<?,Holder<Program>>, Void> prepend(Program program) {
		return (pair) -> {
			if (pair == null) {
				throw new IllegalStateException("No program holder");
			}
			Holder<Program> holder = pair.getValue();
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

	@Override
	public RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits) {
		for (Program program : programs) {
			rel = program.run(planner, rel, requiredOutputTraits);
			logger.debug("After running " + program + ":\n" + RelOptUtil.toString(rel));
		}
		return rel;
	}
}
