package com.atguigu01.mapreduce.join.copy;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Create by chenqinping on 2019/3/6 14 18
 */
public class CopyRJReduce extends Reducer<CopyOrderBean, NullWritable, CopyOrderBean, NullWritable> {

    @Override
    protected void reduce(CopyOrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        Iterator<NullWritable> iterator = values.iterator();
        iterator.next();
        String pnaem = key.getPname();

        while (iterator.hasNext()) {
            iterator.next();
            key.setPname(pnaem);
            context.write(key, NullWritable.get());
        }
    }
}
