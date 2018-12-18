package com.owl.hadoop.mr.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 10:56 2018/12/18
 * @Modified by:
 */
public class ReverseIndexMapper extends Mapper<Object, Text, Text, Text> {

    private Text mapOutputKey = new Text();
    private Text mapOutputValue = new Text();

    private String filePath;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        filePath = fileSplit.getPath().toString();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while (tokenizer.hasMoreTokens()) {
            mapOutputKey.set(tokenizer.nextToken());
            mapOutputValue.set(filePath + ":1");
            context.write(mapOutputKey, mapOutputValue);
        }
    }

}
