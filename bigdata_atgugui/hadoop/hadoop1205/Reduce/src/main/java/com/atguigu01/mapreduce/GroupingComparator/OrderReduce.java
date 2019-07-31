package com.atguigu01.mapreduce.GroupingComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Create by chenqinping on 2019/3/5 15 31
 */
public class OrderReduce extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
//        context.write(key, NullWritable.get());

        Iterator<NullWritable> iterator = values.iterator();

        for (int i = 0; i < 2; i++) {

            if (iterator.hasNext()) {
                context.write(key, iterator.next());
            }

        }
    }


}
