package com.owl.hadoop.mr.leftjoin2;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 16:00 2018/12/7
 * @Modified by:
 */
public class LeftjoinGroupComparator extends WritableComparator {

    public LeftjoinGroupComparator() {
        super(PairOfStrings.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PairOfStrings p1 = (PairOfStrings) a;
        PairOfStrings p2 = (PairOfStrings) b;
        return p1.getLeftElement().compareTo(p2.getLeftElement());
    }
}
