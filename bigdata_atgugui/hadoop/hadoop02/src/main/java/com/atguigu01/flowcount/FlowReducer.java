package com.atguigu01.flowcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Create by chenqingping on ${DATA}
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    FlowBean v =  new FlowBean();

    // key 13726230503 电话号
    // values FlowBean 流量对象
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {

        long sum_upflow = 0;
        long sum_downflow = 0;

        // 1 汇总
        for (FlowBean flowBean : values) {
            sum_upflow += flowBean.getUpFlow();
            sum_downflow += flowBean.getDownFlow();
        }

        // 添加数据
        v.set(sum_upflow, sum_downflow);

        // 2 输出
        context.write(key, v);

    }
}
