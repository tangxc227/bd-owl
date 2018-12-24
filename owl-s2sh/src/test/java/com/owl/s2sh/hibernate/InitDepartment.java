package com.owl.s2sh.hibernate;

import com.owl.s2sh.model.Department;
import com.owl.s2sh.util.SessionFactoryConfig;
import org.hibernate.Session;
import org.junit.Test;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 16:43 2018/12/24
 * @Modified by:
 */
public class InitDepartment {

    @Test
    public void test() {
        Session session = null;
        try {
            session = SessionFactoryConfig.openSession();
            //开启事务
            session.beginTransaction();
            Department dm = new Department();
            dm.setName("行政部");
            //添加操作
            session.save(dm);
            //提交事务
            session.getTransaction().commit();
        } catch (Exception e) {
            if (session != null) {
                session.getTransaction().rollback();
            }
            e.printStackTrace();
        } finally {
            SessionFactoryConfig.close(session);
        }
    }
}
