package com.atguigu01.mapreduce.GroupingComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Create by chenqinping on 2019/3/5 15 28
 */
public class OrderComparator extends WritableComparator {


    protected OrderComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;


        return Double.compare(oa.getPrice(), ob.getPrice());
    }
}
