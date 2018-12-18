package com.owl.hadoop.mr.mongo;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.owl.hadoop.util.HdfsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 14:03 2018/12/18
 * @Modified by:
 */
public class MongoDBRunner extends Configured implements Tool {

    static class PersonMongoDBWritable implements MongoDBWritable {

        private String name;
        private Integer age;
        private String sex = "";
        private int count = 1;

        @Override
        public void readFields(DBObject dbObject) {
            this.name = dbObject.get("name").toString();
            if (dbObject.get("age") != null) {
                this.age = Double.valueOf(dbObject.get("age").toString()).intValue();
            } else {
                this.age = null;
            }
        }

        @Override
        public void write(DBCollection dbCollection) {
            DBObject dbObject = BasicDBObjectBuilder.start().add("age", this.age).add("count", this.count).get();
            dbCollection.insert(dbObject);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.name);
            out.writeUTF(this.sex);
            if (this.age == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeInt(this.age);
            }
            out.writeInt(this.count);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.name = in.readUTF();
            this.sex = in.readUTF();
            if (in.readBoolean()) {
                this.age = in.readInt();
            } else {
                this.age = null;
            }
            this.count = in.readInt();
        }

    }

    static class MongoDBMapper extends Mapper<LongWritable, PersonMongoDBWritable, IntWritable, PersonMongoDBWritable> {

        @Override
        protected void map(LongWritable key, PersonMongoDBWritable value, Context context) throws IOException, InterruptedException {
            if (value.age == null) {
                System.out.println("过滤数据" + value.name);
                return;
            }
            context.write(new IntWritable(value.age), value);
        }

    }

    static class MongoDBReducer extends Reducer<IntWritable, PersonMongoDBWritable, NullWritable, PersonMongoDBWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<PersonMongoDBWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (PersonMongoDBWritable value : values) {
                sum += value.count;
            }
            PersonMongoDBWritable personMongoDBWritable = new PersonMongoDBWritable();
            personMongoDBWritable.age = key.get();
            personMongoDBWritable.count = sum;
            context.write(NullWritable.get(), personMongoDBWritable);
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        // 设置intputformat的value 类
        conf.setClass("mapreduce.mongo.split.value.class", PersonMongoDBWritable.class, MongoDBWritable.class);

        Job job = Job.getInstance(conf, "自定义input/outputformat");
        job.setJarByClass(MongoDBRunner.class);
        job.setMapperClass(MongoDBMapper.class);
        job.setReducerClass(MongoDBReducer.class);
        // mapper输出key
        job.setMapOutputKeyClass(IntWritable.class);
        // mapper输出value
        job.setMapOutputValueClass(PersonMongoDBWritable.class);
        // reducer输出key
        job.setOutputKeyClass(NullWritable.class);
        // reducer 输出value
        job.setOutputValueClass(PersonMongoDBWritable.class);
        // 设置intputformat
        job.setInputFormatClass(MongoDBInputFormat.class);
        // 设置otputformat
        job.setOutputFormatClass(MongoDBOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HdfsUtil.getConfiguration();
        int exitCode = ToolRunner.run(configuration, new MongoDBRunner(), args);
        System.exit(exitCode);
    }

}
