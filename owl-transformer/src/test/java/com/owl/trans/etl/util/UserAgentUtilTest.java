package com.owl.trans.etl.util;

import org.junit.Test;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 10:14 2018/12/24
 * @Modified by:
 */
public class UserAgentUtilTest {

    @Test
    public void test() {
        String userAgent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.71 Safari/537.36";
        System.out.println(UserAgentUtil.analyticUserAgent(userAgent));
    }

}
