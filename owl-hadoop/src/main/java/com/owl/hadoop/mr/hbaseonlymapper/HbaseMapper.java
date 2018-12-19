package com.owl.hadoop.mr.hbaseonlymapper;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 09:46 2018/12/19
 * @Modified by:
 */
public class HbaseMapper extends TableMapper<ImmutableBytesWritable, Put> {

    /**
     * 转换字符串为map对象
     *
     * @param content
     * @return
     */
    static Map<String, String> transfoerContent2Map(String content) {
        Map<String, String> map = new HashMap<String, String>();
        int i = 0;
        String key = "";
        StringTokenizer tokenizer = new StringTokenizer(content, "({|}|\"|:|,)");
        while (tokenizer.hasMoreTokens()) {
            if (++i % 2 == 0) {
                // 当前的值是value
                map.put(key, tokenizer.nextToken());
            } else {
                // 当前的值是key
                key = tokenizer.nextToken();
            }
        }
        return map;
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String content = Bytes.toString(value.getValue(Bytes.toBytes("f"), Bytes.toBytes("content")));
        if (content == null) {
            System.err.println("数据格式错误" + content);
            return;
        }
        Map<String, String> map = HbaseMapper.transfoerContent2Map(content);
        ImmutableBytesWritable outputkey;
        if (map.containsKey("p_id")) {
            // 产品id存在
            outputkey = new ImmutableBytesWritable(Bytes.toBytes(map.get("p_id")));
        } else {
            System.err.println("数据格式错误" + content);
            return;
        }
        Put put = new Put(Bytes.toBytes(map.get("p_id")));
        if (map.containsKey("p_name") && map.containsKey("price")) {
            // 数据正常，进行赋值
            put.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes(map.get("p_id")));
            put.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes(map.get("p_name")));
            put.add(Bytes.toBytes("f"), Bytes.toBytes("price"), Bytes.toBytes(map.get("price")));
        } else {
            System.err.println("数据格式错误" + content);
            return;
        }
        context.write(outputkey, put);
    }
}
