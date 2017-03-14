package org.apache.calcite.adapter.jdbc;

import com.google.common.base.Function;
import org.apache.calcite.adapter.jdbc.programs.ForcedRulesProgram;
import org.apache.calcite.adapter.jdbc.programs.SequenceProgram;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.tools.Program;
import org.apache.calcite.util.Holder;

public class JournalledJdbcRuleManager {
	private static final Program PROGRAM = new ForcedRulesProgram(new JdbcRelBuilder.FactoryFactory(),
			new JournalledInsertRule(),
			new JournalledUpdateRule(),
			new JournalledDeleteRule()
	);

	private static Hook.Closeable globalProgramClosable;

	// Use with Hook.PROGRAM.add
	@SuppressWarnings("Guava") // Must conform to Calcite's API
	public static Function<Holder<Program>, Void> program() {
		return SequenceProgram.prepend(PROGRAM);
	}

	public static synchronized void addHook() {
		if (globalProgramClosable == null) {
			globalProgramClosable = Hook.PROGRAM.add(program());
		}
	}

	public static synchronized void removeHook() {
		if (globalProgramClosable != null) {
			globalProgramClosable.close();
			globalProgramClosable = null;
		}
	}

	private JournalledJdbcRuleManager() {
		throw new UnsupportedOperationException();
	}
}
