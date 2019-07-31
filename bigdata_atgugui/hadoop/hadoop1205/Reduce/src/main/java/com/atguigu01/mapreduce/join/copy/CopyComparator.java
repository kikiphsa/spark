package com.atguigu01.mapreduce.join.copy;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Create by chenqinping on 2019/3/6 14 16
 */
public class CopyComparator extends WritableComparator {

    protected CopyComparator() {
        super(CopyOrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CopyOrderBean oa = (CopyOrderBean) a;
        CopyOrderBean ob = (CopyOrderBean) b;

        return oa.getPid().compareTo(ob.getPid());
    }
}
