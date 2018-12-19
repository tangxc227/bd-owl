package com.owl.hadoop.mr.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 09:32 2018/12/19
 * @Modified by:
 */
public class HbaseReducer extends TableReducer<Text, ProductModel, ImmutableBytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<ProductModel> values, Context context) throws IOException, InterruptedException {
        for (ProductModel value : values) {
            ImmutableBytesWritable outputKey = new ImmutableBytesWritable(Bytes.toBytes(key.toString()));
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes(value.getId()));
            put.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes(value.getName()));
            put.add(Bytes.toBytes("f"), Bytes.toBytes("price"), Bytes.toBytes(value.getPrice()));
            context.write(outputKey, put);
        }
    }

}
