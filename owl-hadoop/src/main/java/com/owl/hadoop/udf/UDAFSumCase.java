package com.owl.hadoop.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveWritableObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 10:16 2018/12/19
 * @Modified by:
 */
public class UDAFSumCase extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        if (info.isAllColumns()) {
            // 函数允许使用“*”查询的时候会返回true。
            throw new SemanticException("不支持使用*查询");
        }
        // 获取函数参数列表
        ObjectInspector[] inspectors = info.getParameterObjectInspectors();
        if (inspectors.length != 1) {
            throw new UDFArgumentException("只支持一个参数进行查询");
        }
        AbstractPrimitiveWritableObjectInspector apwoi = (AbstractPrimitiveWritableObjectInspector) inspectors[0];
        switch (apwoi.getPrimitiveCategory()) {
            case BYTE:
            case INT:
            case SHORT:
            case LONG:
                // 都进行整型的sum操作
                return new SumLongEvaluator();
            case DOUBLE:
            case FLOAT:
                // 进行浮点型的sum操作
                return new SumDoubleEvaluator();
            default:
                throw new UDFArgumentException("参数异常");
        }
    }

    static class SumDoubleEvaluator extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector inputOI;

        static class SumDoubleAgg implements AggregationBuffer {
            double sum;
            boolean empty;
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            this.inputOI = (PrimitiveObjectInspector) parameters[0];
            return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            SumDoubleAgg sda = new SumDoubleAgg();
            this.reset(sda);
            return sda;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            SumDoubleAgg sda = (SumDoubleAgg) agg;
            sda.empty = true;
            sda.sum = 0;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            this.merge(agg, parameters[0]);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return this.terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                SumDoubleAgg sda = (SumDoubleAgg) agg;
                sda.sum += PrimitiveObjectInspectorUtils.getDouble(partial, inputOI);
                sda.empty = false;
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            SumDoubleAgg sda = (SumDoubleAgg) agg;
            if (sda.empty) {
                return null;
            }
            return new DoubleWritable(sda.sum);
        }

    }

    static class SumLongEvaluator extends GenericUDAFEvaluator {

        private PrimitiveObjectInspector inputOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if (parameters.length != 1) {
                throw new UDFArgumentException("参数异常");
            }
            inputOI = (PrimitiveObjectInspector) parameters[0];
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        static class SumLongAgg implements AggregationBuffer {
            long sum;
            boolean empty;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            SumLongAgg sla = new SumLongAgg();
            this.reset(sla);
            return sla;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            SumLongAgg sla = (SumLongAgg) agg;
            sla.sum = 0;
            sla.empty = true;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters.length != 1) {
                throw new UDFArgumentException("参数异常");
            }
            this.merge(agg, parameters[0]);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return this.terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                SumLongAgg sla = (SumLongAgg) agg;
                sla.sum += PrimitiveObjectInspectorUtils.getLong(partial, inputOI);
                sla.empty = false;
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            SumLongAgg sla = (SumLongAgg) agg;
            if (sla.empty) {
                return null;
            }
            return new LongWritable(sla.sum);
        }

    }
}
