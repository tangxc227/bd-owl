package com.owl.s2sh.hibernate;

import com.owl.s2sh.model.Department;
import com.owl.s2sh.model.Employee;
import com.owl.s2sh.util.SessionFactoryConfig;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.junit.Test;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 16:56 2018/12/24
 * @Modified by:
 */
public class TestManyToOne {

    @Test
    public void test(){
        Session session = null;
        try {
            Employee em = new Employee("吴用2", "321", "智多星", 800);
            Department dept = new Department();
            dept.setName("财务部2");
            Employee em1 = new Employee("诸葛亮2", "111", "孔明", 1000);
            Department dept1 = new Department();
            dept1.setName("研发部2");

            session = SessionFactoryConfig.openSession();
            session.beginTransaction();

            em.setDepartment(dept);
            em1.setDepartment(dept1);
            /**
             * 结论：应该先保存主对象，再保存从对象
             * 按以下顺序执行，会出现两条update语句
             */
            session.save(em);  //保存雇员
            session.save(em1);
            session.save(dept);   //保存部门
            session.save(dept1);
            session.getTransaction().commit();
        } catch (HibernateException e) {
            if(session != null) session.getTransaction().rollback();
            e.printStackTrace();
        } finally{
            SessionFactoryConfig.close(session);
        }
    }

}
