package com.atguigu01.mapreduce.MP.MP2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * Create by chenqinping on 2019/3/11 10 18
 */
public class TopNNReduce extends Reducer<FlowBean, Text, Text, FlowBean> {

    TreeMap<FlowBean, Text> flowMap = new TreeMap<FlowBean, Text>();


    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            FlowBean bean = new FlowBean();
            bean.set(key.getUpFlow(), key.getDownFlow());

            flowMap.put(bean, new Text(value));
        }

        if (flowMap.size() > 10) {
            flowMap.remove(flowMap.lastKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        Iterator<FlowBean> iterator = flowMap.keySet().iterator();
        while (iterator.hasNext()) {
            FlowBean next = iterator.next();
            context.write(new Text(flowMap.get(next)), next);
        }
    }
}
