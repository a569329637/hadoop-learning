package com.gsq.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

/**
 * @author gsq
 * @date 2020/10/19
 */
public class MySum extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        //判断参数的个数是否符合要求
        if (info.length != 1) {
            throw new UDFArgumentTypeException(info.length - 1, "exactly one parameter expected");
        }

        //判断传入的参数类型
        if (info[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "only primitive argument is expected but " +
                    info[0].getTypeName() + "is passed");
        }

        //对传入的参数类型进行进一步的判断是否是我们需求的数据的类型
        switch (((PrimitiveTypeInfo) info[0]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                return new SumRes();
            default:
                throw new UDFArgumentTypeException(0, "only numric type is expected but " + info[0].getTypeName() + "is passed");
        }

    }

    public static class SumRes extends GenericUDAFEvaluator {

        //创建变量存储中间结果
        //input:每一步执行时传入的参数
        //output:每一步执行时输出的结果数据的类型
        //input和output都只是指定的输入输出的数据类型而已,和数据计算本身无关
        //result是聚合的结果的数据,和用于particial2和final阶段的结果输出,genuine不同的业务要求指定不同的类型等
        private PrimitiveObjectInspector input;
        private PrimitiveObjectInspector output;
        private LongWritable result;

        //对各个阶段都会首先调用一下该方法,并且对输入输出数据初始化

        /**
         * Mode:
         * partial1 : map阶段                会调用 init -> iterate -> partialterminate
         * partial2 : combiner阶段           会调用 init -> merge -> partialterminate
         * final    : reduce阶段             会调用 init -> merge -> terminate
         * complete : 只有map没有reduce阶段   会调用 init -> iterate -> terminate
         */
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert parameters.length == 1;
            super.init(m, parameters);

            //init input
            //将传入的参数赋值给定义的input输入变量
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                input = (PrimitiveObjectInspector) parameters[0];
            } else {
                input = (PrimitiveObjectInspector) parameters[0];
            }

            //init output
            //返回中间聚合,或最终结果的数据的类型
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                output = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
            } else {
                output = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
            }
            //result用于实际接收聚合结果数据
            result = new LongWritable();
            return output;
        }


        //中间缓存的暂存结构,用于接收中间运行时需要暂存的变量数据
        static class AggregateAgg implements AggregationBuffer {
            Long sum;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AggregateAgg result = new AggregateAgg();
            reset(result);
            return result;
        }

        //刷新缓存重置暂存数据,重用jvm
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            AggregateAgg myAgg = (AggregateAgg) agg;
            myAgg.sum = 0L;
        }

        //对map端传入的每一条数据进行处理
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert parameters.length == 1;
            Object param = parameters[0];
            if (param != null) {
                AggregateAgg myAgg = (AggregateAgg) agg;
                myAgg.sum++;
            }
        }

        //返回map阶段对每一条数据处理后的数据
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            AggregateAgg myAgg = (AggregateAgg) agg;
            result.set(myAgg.sum);
            return result;
        }

        //在combiner和reduce时候回调用,对map输出的结果进行聚合,即每一条数据调用一下,依次将数据累加到之前的结果上
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                AggregateAgg myAgg = (AggregateAgg) agg;
                myAgg.sum += PrimitiveObjectInspectorUtils.getLong(partial, input);
            }
        }

        //使用变量接收最终的结果数据,并将数据进行返回
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            AggregateAgg myAgg = (AggregateAgg) agg;
            result.set(myAgg.sum);
            return result;
        }
    }
}