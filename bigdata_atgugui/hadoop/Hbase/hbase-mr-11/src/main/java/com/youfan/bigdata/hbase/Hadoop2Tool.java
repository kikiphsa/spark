package com.youfan.bigdata.hbase;

import com.youfan.bigdata.hbase.tool.ToolHadoop;
import org.apache.hadoop.util.ToolRunner;

/**
 * Create by chenqinping on 2019/4/8 19:21
 */
public class Hadoop2Tool{

    public static void main(String[] args) throws Exception {

        ToolRunner.run(new ToolHadoop(),args);
    }
}
