package com.owl.hadoop.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 10:35 2018/12/18
 * @Modified by:
 */
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text mapOutputKey = new Text();
    private IntWritable mapOutputValue = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while (tokenizer.hasMoreTokens()) {
            mapOutputKey.set(tokenizer.nextToken());
            context.write(mapOutputKey, mapOutputValue);
        }
    }

}
