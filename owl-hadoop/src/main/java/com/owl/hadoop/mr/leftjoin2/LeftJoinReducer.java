package com.owl.hadoop.mr.leftjoin2;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 15:07 2018/12/7
 * @Modified by:
 */
public class LeftJoinReducer extends Reducer<PairOfStrings, PairOfStrings, NullWritable, Text> {

    private Text outputValue = new Text();

    @Override
    protected void reduce(PairOfStrings key, Iterable<PairOfStrings> values, Context context) throws IOException, InterruptedException {
        String customerInfo = null;
        Iterator<PairOfStrings> iterator = values.iterator();
        if (iterator.hasNext()) {
            PairOfStrings firstPair = iterator.next();
            if ("C".equals(firstPair.getLeftElement())) {
                customerInfo = firstPair.getRightElement();
            }
        }
        while (iterator.hasNext()) {
            PairOfStrings secondPair = iterator.next();
            if ("O".equals(secondPair.getLeftElement())) {
                String orderInfo = secondPair.getRightElement();
                outputValue.set(orderInfo + "," + customerInfo);
                context.write(NullWritable.get(), outputValue);
            }
        }
    }

}
