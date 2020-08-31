package com.gsq.mr.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IndexTwoReducer extends Reducer<Text, Text, Text, Text> {

    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);

        StringBuilder sb = new StringBuilder();
        for (Text text : values) {
            sb.append(text.toString()).append(" ");
        }
        v.set(sb.toString());

        context.write(key, v);
    }
}
