package com.owl.hadoop.mr.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 15:04 2018/12/6
 * @Modified by:
 */
public class SecondarySortMapper extends Mapper<Object, Text, PairWritable, Text> {

    private PairWritable mapOutputKey = new PairWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        if (tokens.length != 2) {
            return;
        }
        mapOutputKey.set(tokens[0], Integer.valueOf(tokens[1]));
        context.write(mapOutputKey, value);
    }
}
