package com.atguigu01.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Create by chenqingping on ${DATA}
 */
public class OrderPartition extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean key, NullWritable nullWritable, int i) {
        return (key.getOrder_id() & Integer.MAX_VALUE) % i;
    }
}
