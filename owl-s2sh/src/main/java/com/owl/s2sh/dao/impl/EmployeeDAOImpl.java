package com.owl.s2sh.dao.impl;

import com.owl.s2sh.dao.EmployeeDAO;
import com.owl.s2sh.model.Employee;
import com.owl.s2sh.util.SessionFactoryConfig;
import org.hibernate.Session;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 16:28 2018/12/24
 * @Modified by:
 */
public class EmployeeDAOImpl implements EmployeeDAO {

    @Override
    public Employee load(int id) {
        Session session = null;
        Employee employee = null;
        try {
            session = SessionFactoryConfig.openSession();
            employee = session.load(Employee.class, id);
        } catch (Exception e) {
            e.printStackTrace();
        } finally{
            SessionFactoryConfig.close(session);
        }
        return employee;
    }

}
