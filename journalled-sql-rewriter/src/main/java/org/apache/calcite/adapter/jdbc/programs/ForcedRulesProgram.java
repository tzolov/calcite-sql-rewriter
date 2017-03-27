/*
 * Copyright 2012-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.jdbc.programs;

import java.util.List;

import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactory;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactoryFactory;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;
import org.apache.calcite.util.Litmus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForcedRulesProgram implements Program {
	private static final Logger logger = LoggerFactory.getLogger(ForcedRulesProgram.class);

	private final JdbcRelBuilderFactoryFactory relBuilderFactoryFactory;

	private final ForcedRule[] rules;

	public ForcedRulesProgram(JdbcRelBuilderFactoryFactory relBuilderFactoryFactory, ForcedRule... rules) {
		this.relBuilderFactoryFactory = relBuilderFactoryFactory;
		this.rules = rules;
	}

	@Override
	public RelNode run(RelOptPlanner relOptPlanner,
			RelNode relNode,
			RelTraitSet relTraitSet,
			List<RelOptMaterialization> relOptMaterializationList,
			List<RelOptLattice> list1) {

		logger.debug("Running forced rules on:\n" + RelOptUtil.toString(relNode));

		return replace(relNode, rules, relBuilderFactoryFactory.create(relOptPlanner.getContext()));
	}

	private RelNode replace(RelNode original, ForcedRule[] rules, JdbcRelBuilderFactory relBuilderFactory) {
		RelNode p = original;
		for (ForcedRule rule : rules) {
			RelNode updated = rule.apply(p, relBuilderFactory);
			if (updated != null) {
				logger.trace("Rule: " + rule.toString() +
						"\nReplacing:\n" + RelOptUtil.toString(p) +
						"\nWith:\n" + RelOptUtil.toString(updated)
				);
				// Must maintain row types so that nothing explodes
				RelOptUtil.equal(
						"RowType of original", p.getRowType(),
						"RowType of replaced", updated.getRowType(),
						Litmus.THROW
				);
				p = updated;
				break;
			}
		}

		List<RelNode> oldInputs = p.getInputs();
		for (int i = 0; i < oldInputs.size(); i++) {
			RelNode originalInput = oldInputs.get(i);
			RelNode replacedInput = replace(originalInput, rules, relBuilderFactory);
			if (replacedInput != originalInput) {
				p.replaceInput(i, replacedInput);
			}
		}
		return p;
	}
}
