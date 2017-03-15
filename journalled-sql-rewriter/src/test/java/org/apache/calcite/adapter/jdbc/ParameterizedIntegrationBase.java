package org.apache.calcite.adapter.jdbc;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@SuppressWarnings("WeakerAccess") // must be public for JUnit access
@RunWith(Parameterized.class)
public abstract class ParameterizedIntegrationBase extends IntegrationBase {
	@Parameterized.Parameters(name = "{index}: {0}")
	public static Collection<Object[]> data() {
		List<Object[]> result = new ArrayList<>();
		for(JournalVersionType vt : JournalVersionType.values()) {
			result.add(new Object[] {vt});
		}
		return result;
	}

	protected ParameterizedIntegrationBase(JournalVersionType versionType, boolean requiresCalcite1692Workaround) {
		super(versionType, requiresCalcite1692Workaround);
	}
}
