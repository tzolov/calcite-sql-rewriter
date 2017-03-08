package io.pivotal.beach.calcite.programs;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilderFactory;

public interface BasicForcedRule {
	RelNode apply(RelNode originalScan, RelBuilderFactory relBuilderFactory);
}
