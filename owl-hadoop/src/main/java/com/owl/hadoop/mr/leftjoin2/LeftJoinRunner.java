package com.owl.hadoop.mr.leftjoin2;

import com.owl.hadoop.util.HdfsUtil;
import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 15:07 2018/12/7
 * @Modified by:
 */
public class LeftJoinRunner extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = this.getConf();

        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, OrderMapper.class);
        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(PairOfStrings.class);

        job.setPartitionerClass(LeftjoinPartitioner.class);
        job.setGroupingComparatorClass(LeftjoinGroupComparator.class);
        job.setSortComparatorClass(PairOfStrings.Comparator.class);

        job.setReducerClass(LeftJoinReducer.class);
        job.setOutputKeyClass(NullPointerException.class);
        job.setOutputValueClass(Text.class);

        Path outputPath = new Path(args[2]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: DataJoinRunner.jar <input1> <input2> <output>");
            System.exit(1);
        }
        Configuration configuration = HdfsUtil.getConfiguration();
        int exitCode = ToolRunner.run(configuration, new LeftJoinRunner(), args);
        System.exit(exitCode);
    }

}
