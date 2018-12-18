package com.owl.hadoop.mr.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 15:16 2018/12/6
 * @Modified by:
 */
public class SecondarySortPartitioner extends Partitioner<PairWritable, Text> {

    @Override
    public int getPartition(PairWritable pairWritable, Text text, int numPartitions) {
        return Math.abs(pairWritable.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }

}
