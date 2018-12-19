package com.owl.hadoop.mr.hbase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @Author: tangxc
 * @Description: 从Hbase获取数据
 * @Date: Created in 09:05 2018/12/19
 * @Modified by:
 */
public class HbaseMapper extends TableMapper<Text, ProductModel> {

    private Text mapOutputKey = new Text();
    private ProductModel mapOutputValue = new ProductModel();

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String content = Bytes.toString(value.getValue(Bytes.toBytes("f"), Bytes.toBytes("content")));
        if (StringUtils.isBlank(content)) {
            System.err.println("数据格式错误：" + content);
            return;
        }
        try {
            JSONObject object = JSON.parseObject(content);
            if (object.containsKey("p_id")) {
                mapOutputKey.set(object.getString("p_id"));
            } else {
                return;
            }
            if (object.containsKey("p_name") && object.containsKey("price")) {
                // 数据正常，进行赋值
                mapOutputValue.set(mapOutputKey.toString(), object.getString("p_name"), object.getString("price"));
            } else {
                return;
            }
            context.write(mapOutputKey, mapOutputValue);
        } catch (Exception e) {
            // nothing
        }


    }

}
