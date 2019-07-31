package com.youfan.bigdata.hbase.mr.mapper;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/4/8 11:22
 */
public class ReadFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String lineValue = value.toString();

        String[] split = lineValue.split(",");
        String rowkey = split[0];
        String name = split[1];

        ImmutableBytesWritable writable = new ImmutableBytesWritable(Bytes.toBytes(rowkey));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name));

        context.write(writable, put);
    }
}
