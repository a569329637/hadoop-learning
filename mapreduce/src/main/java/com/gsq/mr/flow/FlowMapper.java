package com.gsq.mr.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        String line = value.toString();
        String[] fields = line.split("\t");

        k.set(fields[1]);

        String upFlow = fields[fields.length - 3];
        String downFlow = fields[fields.length - 2];
        v.setUpFlow(Long.parseLong(upFlow));
        v.setDownFlow(Long.parseLong(downFlow));
        //v.setSumFlow(v.getUpFlow() + v.getDownFlow());

        context.write(k, v);
    }
}
