package org.apache.calcite.adapter.jdbc;

@SuppressWarnings("WeakerAccess") // protected makes more sense than package-private
abstract class IntegrationBase {
	protected final JournalVersionType versionType;
	protected final String virtualSchemaName;
	protected final String actualSchemaName;

	protected IntegrationBase(JournalVersionType versionType, boolean requiresCalcite1692Workaround) {
		this.versionType = versionType;
		if (requiresCalcite1692Workaround) {
			this.virtualSchemaName = TargetDatabase.getActualSchema(versionType);
		} else {
			this.virtualSchemaName = TargetDatabase.getVirtualSchema(versionType);
		}
		this.actualSchemaName = TargetDatabase.getActualSchema(versionType);

		JournalledJdbcSchema.Factory.INSTANCE.setAutomaticallyAddRules(false);
	}
}
