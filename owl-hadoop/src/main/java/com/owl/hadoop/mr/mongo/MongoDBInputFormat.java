package com.owl.hadoop.mr.mongo;

import com.mongodb.*;
import com.owl.hadoop.configuration.ConfigurationManager;
import com.owl.hadoop.constant.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: tangxc
 * @Description: 自定义MongoDB Inputformat
 * @Date: Created in 11:41 2018/12/18
 * @Modified by:
 */
public class MongoDBInputFormat<V extends MongoDBWritable> extends InputFormat<LongWritable, V> {

    /**
     * MongoDB自定义InputSplit
     */
    static class MongoDBInputSplit extends InputSplit implements Writable {

        /**
         * 开始位置，包含
         */
        private long start;
        /**
         * 结束位置，不包含
         */
        private long end;

        public MongoDBInputSplit() {

        }

        public MongoDBInputSplit(long start, long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(this.start);
            out.writeLong(this.end);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.start = in.readLong();
            this.end = in.readLong();
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return this.end - this.start;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }

    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        // 获取mongo连接
        DB mongo = Mongo.connect(new DBAddress(ConfigurationManager.getProperty(Constants.MONGO_HOST),
                ConfigurationManager.getProperty(Constants.MONGO_DB_NAME)));
        // 获取mongo集合
        DBCollection dbCollection = mongo.getCollection("persons");
        // 每两条数据一个mapper
        int chunkSize = 2;
        // 总记录数
        long totalCnt = dbCollection.count();
        // 计算mapper个数
        long chunk = totalCnt / chunkSize;
        List<InputSplit> inputSplits = new ArrayList<>();
        for (int i = 0; i < chunk; i ++) {
            if (i == chunk - 1) {
                inputSplits.add(new MongoDBInputSplit(i * chunkSize, totalCnt));
            } else {
                inputSplits.add(new MongoDBInputSplit(i * chunkSize, (i + 1) * chunkSize));
            }
        }
        return inputSplits;
    }

    @Override
    public RecordReader<LongWritable, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new MongoDBRecordReader<>(split, context);
    }

    static class NullMongoDBWritable implements MongoDBWritable {

        @Override
        public void readFields(DBObject dbObject) {

        }

        @Override
        public void write(DBCollection dbCollection) {

        }

        @Override
        public void write(DataOutput out) throws IOException {

        }

        @Override
        public void readFields(DataInput in) throws IOException {

        }

    }

    static class MongoDBRecordReader<V extends MongoDBWritable> extends RecordReader<LongWritable, V> {

        private MongoDBInputSplit inputSplit;
        private int index;
        private DBCursor dbCursor;
        private LongWritable key;
        private V value;

        public MongoDBRecordReader() {
        }

        public MongoDBRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            this.initialize(split, context);
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            this.inputSplit = (MongoDBInputSplit) split;
            Configuration configuration = context.getConfiguration();
            this.key = new LongWritable();
            Class clz = configuration.getClass("mapreduce.mongo.split.value.class", NullMongoDBWritable.class);
            this.value = (V) ReflectionUtils.newInstance(clz, configuration);
            this.index = 0;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (this.dbCursor == null) {
                DB mongo = Mongo.connect(new DBAddress(
                        ConfigurationManager.getProperty(Constants.MONGO_HOST),
                        ConfigurationManager.getProperty(Constants.MONGO_DB_NAME)
                ));
                DBCollection dbCollection = mongo.getCollection("persons");
                this.dbCursor = dbCollection.find().skip((int) this.inputSplit.start)
                        .limit((int) this.inputSplit.getLength());
            }
            boolean hasNext = dbCursor.hasNext();
            if (hasNext) {
                DBObject dbObject = this.dbCursor.next();
                this.key.set(this.inputSplit.start + index);
                this.index++;
                this.value.readFields(dbObject);
            }
            return hasNext;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return this.key;
        }

        @Override
        public V getCurrentValue() throws IOException, InterruptedException {
            return this.value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void close() throws IOException {
            this.dbCursor.close();
        }

    }

}
