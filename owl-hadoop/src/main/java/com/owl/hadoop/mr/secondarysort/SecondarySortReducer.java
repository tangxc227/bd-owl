package com.owl.hadoop.mr.secondarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 15:04 2018/12/6
 * @Modified by:
 */
public class SecondarySortReducer extends Reducer<PairWritable, Text, NullWritable, Text> {

    @Override
    protected void reduce(PairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(NullWritable.get(), value);
        }
    }

}
