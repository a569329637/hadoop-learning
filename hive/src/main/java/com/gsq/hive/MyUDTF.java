package com.gsq.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * select ename, mytable.c1, mytable.c2, mytable.c3
 * from emp lateral view myudtf(hiredate) mytable as c1, c2, c3;
 *
 * select myudtf(hiredate) as (col1, col2) from emp;
 *
 * @author gsq
 * @date 2020/10/19
 */
public class MyUDTF extends GenericUDTF {

    /**
     * 对传入的参数进行初始化
     * 判断参数个数/类型
     * 初始化表结构
     */
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 1) {
            throw new UDFArgumentLengthException("actuly only one argument is expected");
        }
        if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "type of String is expected but " + argOIs[0].getTypeName() + "is passed");
        }

        //初始化表结构
        //创建数组列表存储表字段
        List<String> fieldNames = new LinkedList<String>();
        List<ObjectInspector> fieldIOs = new LinkedList<ObjectInspector>();

        //表字段
        fieldNames.add("year");
        fieldNames.add("month");
        fieldNames.add("day");
        //表字段数据类型
        fieldIOs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldIOs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldIOs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        //将表结构两部分聚合在一起
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldIOs);
    }

    /**
     * 对数据处理的代码
     * 如果是多列的话,可以将每一行的数据存入数组中,然后将数组传入forward,
     * forward每调用一次都会产生一行数据
     */
    @Override
    public void process(Object[] args) throws HiveException {
        String str = args[0].toString();
        String[] splited = str.split("-");
        forward(splited);
        forward(new String[]{"0000", "00", "00"});
//        for (int i = 0; i < splited.length; i++) {
//            try {
//                String[] res = splited[i].split(",");
//                forward(res);
//            } catch (Exception e) {
//                continue;
//            }
//
//        }
    }

    //方法调用完毕时关闭方法
    @Override
    public void close() throws HiveException {

    }
}

