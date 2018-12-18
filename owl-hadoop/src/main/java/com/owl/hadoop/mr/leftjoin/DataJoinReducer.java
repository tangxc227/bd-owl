package com.owl.hadoop.mr.leftjoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 14:49 2018/12/7
 * @Modified by:
 */
public class DataJoinReducer extends Reducer<LongWritable, DataJoinWritable, NullWritable, Text> {

    private Text outputValue = new Text();

    @Override
    protected void reduce(LongWritable key, Iterable<DataJoinWritable> values, Context context) throws IOException, InterruptedException {
        // 用户信息为null
        String customerInfo = null;
        // 集合用于存放订单信息
        List<String> orderList = new ArrayList<>();

        for (DataJoinWritable value : values) {
            if ("customer".equals(value.getTag())) {
                customerInfo = value.getData();
            } else if ("order".equals(value.getTag())) {
                orderList.add(value.getData());
            }
        }
        // output
        for (String order : orderList) {
            // ser outout value信息的拼接组合
            outputValue.set(key.get() + "," + customerInfo + "," + order);
            // output
            context.write(NullWritable.get(), outputValue);
        }
    }

}
