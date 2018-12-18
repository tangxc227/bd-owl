package com.owl.hadoop.configuration;

import java.io.InputStream;
import java.util.Properties;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 11:53 2018/12/18
 * @Modified by:
 */
public class ConfigurationManager {

    private static Properties properties = new Properties();

    static {
        try {
            InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("config.properties");
            properties.load(in);
        } catch (Exception e) {
            throw new RuntimeException("加载配置文件失败");
        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

}
