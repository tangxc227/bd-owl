package com.owl.hadoop.mr.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 15:28 2018/12/6
 * @Modified by:
 */
public class SecondarySortGrouping extends WritableComparator {

    public SecondarySortGrouping() {
        super(PairWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PairWritable p1 = (PairWritable) a;
        PairWritable p2 = (PairWritable) b;
        return p1.getFirst().compareTo(p2.getFirst());
    }

}
