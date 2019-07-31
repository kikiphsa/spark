package com.atguigu01.mapreduce.copy;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/3/5 11 18
 */
public class CopyReduce extends Reducer<Text, Copy1Bean, Text, Copy1Bean> {

    Copy1Bean copy1Bean = new Copy1Bean();

    @Override
    protected void reduce(Text key, Iterable<Copy1Bean> values, Context context) throws IOException, InterruptedException {
        int upFlow = 0;
        int downFlow = 0;
        for (Copy1Bean bean : values) {
            upFlow += bean.getUpFlow();
            downFlow += bean.getDownFlow();
        }
        copy1Bean.set(upFlow, downFlow);

        context.write(key, copy1Bean);
    }
}
