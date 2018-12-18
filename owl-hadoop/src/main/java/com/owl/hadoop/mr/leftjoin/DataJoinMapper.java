package com.owl.hadoop.mr.leftjoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 14:47 2018/12/7
 * @Modified by:
 */
public class DataJoinMapper extends Mapper<Object, Text, LongWritable, DataJoinWritable> {

    /**
     * map output key
     */
    private LongWritable mapOutputKey = new LongWritable();
    /**
     * map output value
     */
    private DataJoinWritable mapOutputValue = new DataJoinWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // line value获取每一行的数据
        String lineValue = value.toString();
        // split分割切片，逗号隔开
        String[] vals = lineValue.split(",");
        int length = vals.length;
        // 首先判断数据是否符合，如果不是3也不是4的话直接返回
        if ((3 != length) && (4 != length)) {
            return;
        }
        // get cid 获取ID
        Long cid = Long.valueOf(vals[0]);
        // get name，用户名称&商品名称，name
        String name = vals[1];

        // set customer，数据长度为3的话，获取相关信息
        if (3 == length) {
            String phone = vals[2];
            // set
            mapOutputKey.set(cid);
            mapOutputValue.set("customer", name + "," + phone);
        }
        // set order数据长度为4的话，获取相关信息
        if (4 == length) {
            String price = vals[2];
            String date = vals[3];

            // set
            mapOutputKey.set(cid);
            mapOutputValue.set("order", name + "," + price + "," + date);
        }
        // output
        context.write(mapOutputKey, mapOutputValue);
    }

}
