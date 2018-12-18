package com.owl.hadoop.mr.index;

import com.owl.hadoop.util.HdfsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 11:16 2018/12/18
 * @Modified by:
 */
public class ReverseIndexRunner extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ReverseIndexRunner.jar <input> <output>");
            System.exit(1);
        }
        Configuration configuration = HdfsUtil.getConfiguration();
        int exitCode = ToolRunner.run(configuration, new ReverseIndexRunner(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = this.getConf();
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        // input
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // mapper
        job.setMapperClass(ReverseIndexMapper.class);

        // reducer
        job.setReducerClass(ReverseIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // output
        HdfsUtil.delete(configuration, args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
