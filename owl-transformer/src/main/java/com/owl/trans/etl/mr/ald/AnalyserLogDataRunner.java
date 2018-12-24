package com.owl.trans.etl.mr.ald;

import com.owl.trans.common.EventLogConstants;
import com.owl.trans.common.GlobalConstants;
import com.owl.trans.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 11:08 2018/12/24
 * @Modified by:
 */
public class AnalyserLogDataRunner extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://127.0.0.1:8020");
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        int exitCode = ToolRunner.run(configuration, new AnalyserLogDataRunner(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = this.getConf();
        this.processArgs(configuration, args);

        Job job = Job.getInstance(configuration, "analyser_logdata");
        job.setJarByClass(AnalyserLogDataRunner.class);

        job.setMapperClass(AnalyserLogDataMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);

        // 设置reducer配置
        // 1. 集群上运行，打成jar运行(要求addDependencyJars参数为true，默认就是true)
        // TableMapReduceUtil.initTableReducerJob(EventLogConstants.HBASE_NAME_EVENT_LOGS, null, job);
        // 2. 本地运行，要求参数addDependencyJars为false
        TableMapReduceUtil.initTableReducerJob(EventLogConstants.HBASE_NAME_EVENT_LOGS, null, job, null, null, null, null, false);

        job.setNumReduceTasks(0);

        this.setJobInputPaths(job);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 参数处理
     *
     * @param configuration
     * @param args
     */
    private void processArgs(Configuration configuration, String[] args) {
        String date = null;
        for (int i = 0; i < args.length; i++) {
            if ("-d".equals(args[i]) && i + 1 < args.length) {
                date = args[++i];
                break;
            }
        }
        if (StringUtils.isBlank(date) && !TimeUtil.isValidateRunningDate(date)) {
            date = TimeUtil.getYesterday();
        }
        configuration.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
    }

    /**
     * 设置job的输入路径
     *
     * @param job
     */
    private void setJobInputPaths(Job job) {
        Configuration configuration = job.getConfiguration();
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(configuration);
            String date = configuration.get(GlobalConstants.RUNNING_DATE_PARAMES);
            Path inputPath = new Path("/logs/" + date);
            if (fileSystem.exists(inputPath)) {
                FileInputFormat.addInputPath(job, inputPath);
            } else {
                throw new RuntimeException("输入目录不存在：" + inputPath);
            }
        } catch (IOException e) {
            throw new RuntimeException("设置job的mapreduce输入路径出现异常", e);
        } finally {
            if (null != fileSystem) {
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    // nothing
                }
            }
        }
    }

}
