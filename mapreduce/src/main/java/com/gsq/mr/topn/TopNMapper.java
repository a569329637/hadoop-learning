package com.gsq.mr.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

public class TopNMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    TreeMap<FlowBean, Text> treeMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //super.setup(context);
        treeMap = new TreeMap<>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // super.map(key, value, context);

        String line = value.toString();
        String[] strings = line.split("\t");

        long upFlow = Long.parseLong(strings[1]);
        long downFlow = Long.parseLong(strings[2]);
        long sumFlow = Long.parseLong(strings[3]);

        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setSumFlow(sumFlow);

        treeMap.put(flowBean, new Text(strings[0]));
        if (treeMap.size() > 10) {
            treeMap.remove(treeMap.lastKey());
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //super.cleanup(context);
        treeMap.forEach((k, v) -> {
            try {
                context.write(k, v);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
