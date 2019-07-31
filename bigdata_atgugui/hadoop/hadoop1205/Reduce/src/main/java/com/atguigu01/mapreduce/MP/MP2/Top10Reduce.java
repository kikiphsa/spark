package com.atguigu01.mapreduce.MP.MP2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Create by chenqinping on 2019/3/11 11 15
 */
public class Top10Reduce extends Reducer<FlowBean, Text, Text, FlowBean> {


    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Iterator<Text> iterator = values.iterator();

        for (int i = 0; i < 10; i++) {
            while (iterator.hasNext()) {
                context.write(iterator.next(), key);
            }
        }
    }
}
