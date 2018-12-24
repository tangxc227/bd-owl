package com.owl.s2sh.hibernate;

import com.owl.s2sh.model.Department;
import com.owl.s2sh.model.Employee;
import com.owl.s2sh.util.SessionFactoryConfig;
import org.hibernate.Session;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 17:03 2018/12/24
 * @Modified by:
 */
public class TestOneToMany {

    @Test
    public void test(){
        Session session = null;
        try {
            Employee emp1 = new Employee("孙权","333","孙仲谋",10000);
            Employee emp2 = new Employee("李世明","113","唐太宗",20000);
            Department dept = new Department();
            dept.setName("军机处");
            //创建集合进行保存
            Set<Employee> emps = new HashSet<>();
            emps.add(emp1);
            emps.add(emp2);
            dept.setEmps(emps);
            session = SessionFactoryConfig.openSession();
            session.beginTransaction();
            //单独保存雇员
            //session.save(emp1);
            //session.save(emp2);
            //保存部门
            session.save(dept);
            session.getTransaction().commit();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Test
    public void test02(){
        Session session = null;
        try {
            session = SessionFactoryConfig.openSession();
            Department dept = session.load(Department.class, 7);
            System.out.println("部门名称："+dept.getName()+",有"+dept.getEmps().size()+"位雇员");
        } catch (Exception e) {
            e.printStackTrace();
        } finally{
            SessionFactoryConfig.close(session);
        }
    }

}
