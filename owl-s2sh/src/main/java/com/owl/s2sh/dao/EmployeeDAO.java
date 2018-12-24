package com.owl.s2sh.dao;

import com.owl.s2sh.model.Employee;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 16:27 2018/12/24
 * @Modified by:
 */
public interface EmployeeDAO {

    /**
     * 根据ID查询员工信息
     *
     * @param id
     * @return
     */
    Employee load(int id);

}
