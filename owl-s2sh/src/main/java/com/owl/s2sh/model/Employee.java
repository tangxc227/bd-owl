package com.owl.s2sh.model;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 15:58 2018/12/24
 * @Modified by:
 */
public class Employee {

    private int id;
    private String username;
    private String password;
    private String nickname;
    private double salary;
    private Department department;

    public Employee() {
    }

    public Employee(String username, String password, String nickname, double salary) {
        this.username = username;
        this.password = password;
        this.nickname = nickname;
        this.salary = salary;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getNickname() {
        return nickname;
    }

    public void setNickname(String nickname) {
        this.nickname = nickname;
    }

    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }

    public Department getDepartment() {
        return department;
    }

    public void setDepartment(Department department) {
        this.department = department;
    }

}
