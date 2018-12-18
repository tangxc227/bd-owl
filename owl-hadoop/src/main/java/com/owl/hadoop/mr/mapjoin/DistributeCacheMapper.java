package com.owl.hadoop.mr.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 14:02 2018/12/7
 * @Modified by:
 */
public class DistributeCacheMapper extends Mapper<Object, Text, NullWritable, Text> {

    private Map<Integer, String> cacheMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        URI[] cacheFile = context.getCacheFiles();
        Path cachePath = new Path(cacheFile[0]);
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream inputStream = fileSystem.open(cachePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tokens = line.split(",");
            if (tokens.length == 3 && tokens[0].matches("^\\d+$")) {
                cacheMap.put(Integer.valueOf(tokens[0]), line);
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isBlank(line)) {
            return;
        }
        String[] tokens = line.split(",");
        if (tokens.length != 4) {
            return;
        }
        try {
            StringBuffer buffer = new StringBuffer();
            int customerId = Integer.valueOf(tokens[0]);

            buffer.append(tokens[0]).append("\t");
            buffer.append(tokens[1]).append("\t");
            buffer.append(tokens[2]).append("\t");
            buffer.append(tokens[3]);

            if (cacheMap.containsKey(customerId)) {
                String customerInfo = cacheMap.get(customerId);
                String[] customerInfos = customerInfo.split(",");
                buffer.append("\t");
                buffer.append(customerInfos[1]).append("\t");
                buffer.append(customerInfos[2]);
            }
            context.write(NullWritable.get(), new Text(buffer.toString()));
        } catch (Exception e) {
            // TODO:
        }

    }

}
