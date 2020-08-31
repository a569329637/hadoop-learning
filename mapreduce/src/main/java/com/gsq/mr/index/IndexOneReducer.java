package com.gsq.mr.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IndexOneReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    //Text k = new Text();
    IntWritable v = new IntWritable();
    int sum;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        sum = 0;
        for (IntWritable c : values) {
            sum += c.get();
        }
        v.set(sum);
        context.write(key, v);
    }
}
