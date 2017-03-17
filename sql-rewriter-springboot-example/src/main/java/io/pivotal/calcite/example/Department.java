package io.pivotal.calcite.example;

/**
 * Created by tzoloc on 3/16/17.
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
