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
package io.pivotal.calcite.example;

/**
 * Domain model representing the content of the depts table.
 */
public class Department {

	private Long deptno;

	private String departmentName;

	protected Department() {}

	public Department(Long deptno, String departmentName) {
		this.deptno = deptno;
		this.departmentName = departmentName;
	}

	@Override
	public String toString() {
		return "Department{" +
				"deptno=" + deptno +
				", departmentName='" + departmentName + "'}";
	}
}
