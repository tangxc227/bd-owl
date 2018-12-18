package com.owl.hadoop.mr.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 11:01 2018/12/18
 * @Modified by:
 */
public class ReverseIndexReducer extends Reducer<Text, Text, Text, Text> {

    private Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> map = new HashMap<>();
        StringBuilder sb;
        for (Text value : values) {
            sb = new StringBuilder();
            String line = value.toString();
            line = sb.append(line).reverse().toString();
            String[] tokens = line.split(":", 2);
            sb = new StringBuilder();
            String path = sb.append(tokens[1]).reverse().toString();
            int count = Integer.valueOf(tokens[0]);
            if (map.containsKey(path)) {
                map.put(path, map.get(path) + count);
            } else {
                map.put(path, count);
            }
        }
        sb = new StringBuilder();
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            sb.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
        }

        outputValue.set(sb.deleteCharAt(sb.length() - 1).toString());
        context.write(key, outputValue);
    }
}
