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
package org.apache.calcite.adapter.jdbc;

import com.google.common.base.Function;
import org.apache.calcite.adapter.jdbc.programs.ForcedRulesProgram;
import org.apache.calcite.adapter.jdbc.programs.SequenceProgram;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.tools.Program;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Pair;

@SuppressWarnings("WeakerAccess") // Part of API
public class JournalledJdbcRuleManager {
	private static final Program PROGRAM = new ForcedRulesProgram(new JdbcRelBuilder.FactoryFactory(),
			new JournalledInsertRule(),
			new JournalledUpdateRule(),
			new JournalledDeleteRule()
	);

	private static Hook.Closeable globalProgramClosable;

	// Use with Hook.PROGRAM.add
	@SuppressWarnings("Guava") // Must conform to Calcite's API
	public static Function<Pair<?,Holder<Program>>, Void> program() {
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
