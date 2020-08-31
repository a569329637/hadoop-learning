package com.gsq.mr.friend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendTwoReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);

        StringBuilder sb = new StringBuilder();
        for (Text value : values) {
            sb.append(value.toString()).append(" ");
        }

        context.write(key, new Text(sb.toString()));
    }
}
