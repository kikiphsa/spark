package com.atguigu01.mapreduce.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/4 10 16
 */
public class FlowReduce extends Reducer<Text,FlowBean,Text,FlowBean> {

    FlowBean flowBean= new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        int upFlow=0;
        int downFlow=0;
        for (FlowBean flowBean:values){
            upFlow += flowBean.getUpFlow();
            downFlow +=flowBean.getDownFlow();
        }
        flowBean.set(upFlow,downFlow);
        context.write(key,flowBean);

    }
}
