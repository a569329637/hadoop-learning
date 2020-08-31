package com.gsq.mr.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendOneMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);

        String line = value.toString();
        if ("".equals(line)) {
            return;
        }
        String[] strings = line.split(":");

        String[] persons = strings[1].split(",");

        for (String person : persons) {
            context.write(new Text(person), new Text(strings[0]));
        }

    }
}
