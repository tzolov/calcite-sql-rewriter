package io.pivotal.beach.calcite.programs;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Litmus;

import java.util.List;

public class ForcedRulesProgram implements Program {
	private final BasicForcedRule[] rules;

	public ForcedRulesProgram(BasicForcedRule... rules) {
		this.rules = rules;
	}

	public RelNode run(
			RelOptPlanner planner,
			RelNode rel,
			RelTraitSet requiredOutputTraits,
			List<RelOptMaterialization> materializations,
			List<RelOptLattice> lattices
	) {
		System.out.println("BEGIN: \n" + RelOptUtil.toString(rel, SqlExplainLevel.ALL_ATTRIBUTES));
		RelNode result = replace(rel, rules, RelBuilder.proto(planner.getContext()));
		System.out.println("END: \n" + RelOptUtil.toString(result, SqlExplainLevel.ALL_ATTRIBUTES));
		return result;
	}

	private RelNode replace(RelNode original, BasicForcedRule[] rules, RelBuilderFactory relBuilderFactory) {
		RelNode p = original;
		for (BasicForcedRule rule : rules) {
			RelNode updated = rule.apply(p, relBuilderFactory);
			if (updated != null) {
				System.out.println("Rule: " + rule.toString());
				System.out.println("Replacing:\n" + RelOptUtil.toString(p, SqlExplainLevel.ALL_ATTRIBUTES));
				System.out.println("With:\n" + RelOptUtil.toString(updated, SqlExplainLevel.ALL_ATTRIBUTES));
				// Must maintain row types so that nothing explodes
				RelOptUtil.equal(
						"rowtype of original", p.getRowType(),
						"rowtype of replaced", updated.getRowType(),
						Litmus.THROW
				);
				p = updated;
			}
		}

		if (p == original) { // optimisation: avoid changing nodes inside stuff we changed
			List<RelNode> oldInputs = p.getInputs();
			for (int i = 0; i < oldInputs.size(); i++) {
				RelNode originalInput = oldInputs.get(i);
				RelNode replacedInput = replace(originalInput, rules, relBuilderFactory);
				if (replacedInput != originalInput) {
					p.replaceInput(i, replacedInput);
				}
			}
		}
		return p;
	}
}
