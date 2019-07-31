package com.atguigu01.flowcount;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create by chenqingping on ${DATA}
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    FlowBean v = new FlowBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 获取一行内容
        // 1363157985066 13726230503 00-FD-07-A4-72-B8:CMCC 120.196.100.82
        // i02.c.aliimg.com 24 27 2481 24681 200
        String line = value.toString();

        // 2 截取“\t”
        String[] fields = line.split("\t");

        // 3 封装对象 13726230503
        String phone = fields[1];
        // 上行流量
        long upflow = Long.parseLong(fields[fields.length - 3]);
        long downflow = Long.parseLong(fields[fields.length - 2]);

        // 添加数据
        v.set(upflow, downflow);
        k.set(phone);

        // 4 输出
        context.write(k, v);
    }
}
