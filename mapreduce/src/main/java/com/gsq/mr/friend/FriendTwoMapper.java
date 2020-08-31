package com.gsq.mr.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);

        String line = value.toString();
        String[] strings = line.split("\t");

        String[] persons = strings[1].split(",");

        for (int i = 0; i < persons.length; ++i) {
            for (int j = 0; j < persons.length; ++j) {
                context.write(new Text(persons[i] + "-" + persons[j]), new Text(strings[0]));
            }
        }
    }
}
