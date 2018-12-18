package com.owl.hadoop.util;

import com.owl.hadoop.configuration.ConfigurationManager;
import com.owl.hadoop.constant.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 14:24 2018/12/18
 * @Modified by:
 */
public class HbaseUtil {

    /**
     * 获取hbase的配置文件信息
     *
     * @return
     */
    public static Configuration getHbaseConfiguration() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(Constants.HBASE_ZOOKEEPER_QUORUM,
                ConfigurationManager.getProperty(Constants.HBASE_ZOOKEEPER_QUORUM));
        return configuration;
    }

}
