package io.pivotal.beach.calcite.programs;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

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
				p = updated;
			}
		}

		List<RelNode> oldInputs = p.getInputs();
		for (int i = 0; i < oldInputs.size(); i++) {
			p.replaceInput(i, replace(oldInputs.get(i), rules, relBuilderFactory));
		}
		return p;
	}
}
