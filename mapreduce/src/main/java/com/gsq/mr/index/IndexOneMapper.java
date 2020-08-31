package com.gsq.mr.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class IndexOneMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);
    String name;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //super.setup(context);
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);

        System.out.println("key = " + key);
        System.out.println("value = " + value);

        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            k.set(word + "--" + name);
            v.set(1);
            context.write(k, v);
        }

    }
}
