package com.atguigu01.mapreduce.InputFromat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Create by chenqinping on 2019/3/5 09 41
 */
public class MyPartition extends Partitioner<Text, BytesWritable> {
    @Override
    public int getPartition(Text text, BytesWritable bytesWritable, int numPartitions) {

        String phone = text.toString();
        String line = phone.substring(0, 3);

    /*    int partition = 4;

        if ("136".equals(line)) {
            partition = 0;
        } else if ("137".equals(line)) {
            partition = 1;
        } else if ("138".equals(line)) {
            partition = 2;
        } else if ("139".equals(line)) {
            partition = 3;
        }


        return partition;*/

        switch (phone.substring(0, 3)) {

            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }
    }
}
