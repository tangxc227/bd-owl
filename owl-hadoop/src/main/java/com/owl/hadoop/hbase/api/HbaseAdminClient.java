package com.owl.hadoop.hbase.api;

import com.owl.hadoop.util.HbaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 14:30 2018/12/18
 * @Modified by:
 */
public class HbaseAdminClient {

    public static void main(String[] args) throws IOException {
        Configuration configuration = HbaseUtil.getHbaseConfiguration();
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
//        createTable(hBaseAdmin);
        getTableDescribe(hBaseAdmin);
//        deleteTable(hBaseAdmin);
        hBaseAdmin.close();
    }

    /**
     * 创建table
     *
     * @param hBaseAdmin
     * @throws IOException
     */
    public static void createTable(HBaseAdmin hBaseAdmin) throws IOException {
        TableName tableName = TableName.valueOf("users");
        if (!hBaseAdmin.tableExists(tableName)) {
            HTableDescriptor htd = new HTableDescriptor(tableName);
            htd.addFamily(new HColumnDescriptor("f"));
            htd.setMaxFileSize(10000L);
            hBaseAdmin.createTable(htd);
            System.out.println("创建表成功");
        } else {
            System.out.println("表存在");
        }
    }

    /**
     * 获取表信息
     *
     * @param hbAdmin
     * @throws IOException
     */
    public static void getTableDescribe(HBaseAdmin hbAdmin) throws IOException {
        TableName name = TableName.valueOf("users");
        if (hbAdmin.tableExists(name)) {
            HTableDescriptor htd = hbAdmin.getTableDescriptor(name);
            System.out.println(htd);
        } else {
            System.out.println("表不存在");
        }
    }

    /**
     * 删除hbase表
     *
     * @param hbAdmin
     * @throws IOException
     */
    public static void deleteTable(HBaseAdmin hbAdmin) throws IOException {
        TableName name = TableName.valueOf("users");
        if (hbAdmin.tableExists(name)) {
            if (hbAdmin.isTableEnabled(name)) {
                hbAdmin.disableTable(name);
            }
            hbAdmin.deleteTable(name);
            System.out.println("删除成功");
        } else {
            System.out.println("表不存在");
        }
    }

}
