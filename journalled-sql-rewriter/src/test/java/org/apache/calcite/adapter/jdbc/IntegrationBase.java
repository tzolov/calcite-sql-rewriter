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
