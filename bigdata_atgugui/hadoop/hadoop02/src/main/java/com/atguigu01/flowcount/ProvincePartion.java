package com.atguigu01.flowcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * Create by chenqingping on ${DATA}
 */
public class ProvincePartion extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text key, FlowBean flowBean, int i) {
        String preNum = key.toString().substring(0, 3);
        int partition=4;

        if ("136".equals(preNum)){
            partition=0;
        }else if ("137".equals(preNum)){
            partition=1;
        }else if ("138".equals(preNum)){
            partition=2;
        }else if ("139".equals(preNum)){
            partition=3;
        }

        return partition;
    }
}
