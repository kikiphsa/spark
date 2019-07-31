package com.atguigu01.mapreduce.MP.MP2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * Create by chenqinping on 2019/3/11 10 00
 */
public class TopNMapper extends Mapper<LongWritable, Text, FlowBean, Text> {


    FlowBean flowBean;

    TreeMap<FlowBean, Text> flowMap = new TreeMap<FlowBean, Text>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        flowBean = new FlowBean();
        Text v = new Text();

        //获取一行
        String[] fields = value.toString().split("\t");

        String friend = fields[0];

        flowBean.setUpFlow(Long.parseLong(fields[1]));
        flowBean.setDownFlow(Long.parseLong(fields[2]));
        flowBean.setSumFlow(Long.parseLong(fields[3]));

        v.set(friend);

        flowMap.put(flowBean, v);

        if (flowMap.size() > 10){
            flowMap.remove(flowMap.firstKey());
            flowMap.remove(flowMap.lastKey());
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        Iterator<FlowBean> iterator = flowMap.keySet().iterator();
        while (iterator.hasNext()) {
            FlowBean bean = iterator.next();
            context.write(bean, flowMap.get(bean));
        }
    }
}
