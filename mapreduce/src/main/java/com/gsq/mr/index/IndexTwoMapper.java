package com.gsq.mr.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IndexTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        String line = value.toString();

        String[] strings = line.split(" ");

        String[] kv = strings[0].split("--");
        k.set(kv[0]);
        v.set(kv[0] + "->" + strings[1]);

        context.write(k, v);
    }
}
