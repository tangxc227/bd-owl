package com.owl.s2sh.model;

import java.util.Set;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 16:39 2018/12/24
 * @Modified by:
 */
public class Department {

    private String id;
    private String name;
    private Set<Employee> emps;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<Employee> getEmps() {
        return emps;
    }

    public void setEmps(Set<Employee> emps) {
        this.emps = emps;
    }
}
