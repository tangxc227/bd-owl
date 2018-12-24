package com.owl.trans.etl.util;

import org.junit.Test;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 10:18 2018/12/24
 * @Modified by:
 */
public class IPSeekerExtTest {

    @Test
    public void test() {
        String ip = "222.94.195.207";
        System.out.println(new IPSeekerExt().analyticIp(ip));
    }

}
