package com.youfan.bigdata.hbase;

import com.youfan.bigdata.hbase.tool.HBase2MysqlTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Create by chenqinping on 2019/4/8 14:22
 */
public class Hbase2MysqlApplication {

    public static void main(String[] args) throws Exception{
        ToolRunner.run(new HBase2MysqlTool(),args);
    }
}
