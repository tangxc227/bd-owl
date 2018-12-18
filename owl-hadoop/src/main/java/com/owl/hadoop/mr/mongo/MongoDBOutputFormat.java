package com.owl.hadoop.mr.mongo;

import com.mongodb.DB;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.owl.hadoop.configuration.ConfigurationManager;
import com.owl.hadoop.constant.Constants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;

/**
 * @Author: tangxc
 * @Description: 自定义outputformat
 * @Date: Created in 13:55 2018/12/18
 * @Modified by:
 */
public class MongoDBOutputFormat<V extends MongoDBWritable> extends OutputFormat<NullWritable, V> {

    static class MongoDBRecordWriter<V extends MongoDBWritable> extends RecordWriter<NullWritable, V> {

        private DBCollection dbCollection;

        public MongoDBRecordWriter() {

        }

        public MongoDBRecordWriter(TaskAttemptContext context) throws IOException {
            DB mongo = Mongo.connect(new DBAddress(
                    ConfigurationManager.getProperty(Constants.MONGO_HOST),
                    ConfigurationManager.getProperty(Constants.MONGO_DB_NAME)
            ));
            this.dbCollection = mongo.getCollection("result");
        }

        @Override
        public void write(NullWritable key, V value) throws IOException, InterruptedException {
            value.write(this.dbCollection);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        }
    }

    @Override
    public RecordWriter<NullWritable, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MongoDBRecordWriter<>(context);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        // noting
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileOutputCommitter(null, context);
    }

}
