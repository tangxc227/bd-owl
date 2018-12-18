package com.owl.hadoop.mr.leftjoin2;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 15:06 2018/12/7
 * @Modified by:
 */
public class OrderMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {

    private PairOfStrings mapOutputKey = new PairOfStrings();
    private PairOfStrings mapOutputValue = new PairOfStrings();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isBlank(line)) {
            return;
        }
        String[] tokens = line.split(",");
        if (tokens.length == 4 && tokens[0].matches("^\\d+$")) {
            mapOutputKey.set(tokens[0], "2");
            mapOutputValue.set("O", line);
            context.write(mapOutputKey, mapOutputValue);
        }
    }

}
