package com.owl.s2sh.hibernate;

import com.owl.s2sh.model.Employee;
import com.owl.s2sh.util.SessionFactoryConfig;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 16:06 2018/12/24
 * @Modified by:
 */
public class InitEmployeeFirst {

    @Test
    public void test() {
        Configuration cfg = new Configuration().configure();
        SessionFactory sessionFactory = cfg.buildSessionFactory();
        Session session = null;
        try {
            session = sessionFactory.openSession();
            //开启事务
            session.beginTransaction();
            Employee employee = new Employee("黄药师", "123", "东邪", 1200);
            //保存操作
            session.save(employee);
            //提交事务
            session.getTransaction().commit();
        } catch (HibernateException e) {
            if(session != null) session.getTransaction().rollback();
            e.printStackTrace();
        } finally{
            if(session != null) session.close();
        }
    }

    @Test
    public void testUpdate() {
        Session session = null;
        try {
            session = SessionFactoryConfig.openSession();
            // 开启事务
            session.beginTransaction();
            Employee employee = session.load(Employee.class, 1);
            employee.setNickname("黄岛主");
            //修改操作
            session.update(employee);
            //提交事务
            session.getTransaction().commit();
        } catch (Exception e) {
            if(session != null) session.getTransaction().rollback();
            e.printStackTrace();
        } finally {
            SessionFactoryConfig.close(session);
        }
    }

}
