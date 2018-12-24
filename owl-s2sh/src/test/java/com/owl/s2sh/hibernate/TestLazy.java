package com.owl.s2sh.hibernate;

import com.owl.s2sh.dao.EmployeeDAO;
import com.owl.s2sh.dao.impl.EmployeeDAOImpl;
import com.owl.s2sh.model.Employee;
import org.junit.Test;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 16:30 2018/12/24
 * @Modified by:
 */
public class TestLazy {

    /**
     * 此时会抛
     * "org.hibernate.LazyInitializationException: could not initialize proxy - no Session"
     * 异常原因：load是有延迟加载的，只是个代理对象，只有id，要取其他属性值，则要去数据库找，但是返回时session已经被关闭
     * 解决方案：可以使用get处理或者将session存储到ThreadLocal中
     */
    @Test
    public void testLazy() {
        EmployeeDAO employeeDAO = new EmployeeDAOImpl();
        Employee employee = employeeDAO.load(1);
        System.out.println(employee);
    }

}
