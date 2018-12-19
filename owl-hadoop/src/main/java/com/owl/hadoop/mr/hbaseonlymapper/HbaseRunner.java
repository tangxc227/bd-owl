package com.owl.hadoop.mr.hbaseonlymapper;

import com.owl.hadoop.util.HbaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 09:37 2018/12/19
 * @Modified by:
 */
public class HbaseRunner extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HbaseUtil.getHbaseConfiguration();
        int exitCode = ToolRunner.run(configuration, new HbaseRunner(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = this.getConf();
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        // 设置mapper相关，mapper从hbase输入
        // 本地环境，而且fs.defaultFS为集群模式的时候，需要设置addDependencyJars参数为false。
        // addDependencyJars集群中，参数必须为true。
        TableMapReduceUtil.initTableMapperJob("data", new Scan(), HbaseMapper.class, ImmutableBytesWritable.class,
                Put.class, job, false);

        // 设置reducer相关，reducer往hbase输出
        // 本地环境，而且fs.defaultFS为集群模式的时候，需呀设置addDependencyJars参数为false。
        TableMapReduceUtil.initTableReducerJob("online_product", null, job, null, null, null, null, false);
        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
