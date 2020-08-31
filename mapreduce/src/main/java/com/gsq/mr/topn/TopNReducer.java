package com.gsq.mr.topn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class TopNReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    TreeMap<FlowBean, Text> treeMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //super.setup(context);
        treeMap = new TreeMap<>();
    }

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);

        System.out.println("key = " + key);
        for (Text text : values) {
            System.out.println("text.toString() = " + text.toString());

            FlowBean flowBean = new FlowBean(key.getUpFlow(), key.getDownFlow());
            treeMap.put(flowBean, new Text(text.toString()));

            if (treeMap.size() > 10) {
                treeMap.remove(treeMap.lastKey());
            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //super.cleanup(context);
        treeMap.forEach((k, v) -> {
            try {
                context.write(v, k);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
