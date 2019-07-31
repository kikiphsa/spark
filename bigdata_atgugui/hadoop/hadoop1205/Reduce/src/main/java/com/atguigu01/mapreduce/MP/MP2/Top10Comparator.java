package com.atguigu01.mapreduce.MP.MP2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Create by chenqinping on 2019/3/11 10 41
 */
public class Top10Comparator extends WritableComparator {

    protected Top10Comparator() {
        super(FlowBean.class, true);
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        return 0;
    }
}
