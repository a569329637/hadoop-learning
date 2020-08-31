package com.gsq.mr.friend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendOneReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);

        StringBuilder sb = new StringBuilder();
        for (Text text : values) {
            sb.append(text.toString()).append(",");
        }

        context.write(key, new Text(sb.toString()));
    }
}
