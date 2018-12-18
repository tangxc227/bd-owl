package com.owl.hadoop.mr.leftjoin2;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 15:59 2018/12/7
 * @Modified by:
 */
public class LeftjoinPartitioner extends Partitioner<PairOfStrings, PairOfStrings> {


    @Override
    public int getPartition(PairOfStrings pairOfStrings, PairOfStrings pairOfStrings2, int numPartitions) {
        return pairOfStrings.getLeftElement().hashCode() & Integer.MAX_VALUE % numPartitions;
    }

}
