package com.youfan.bigdata.hbase;

import org.apache.hadoop.util.ToolRunner;

/**
 * Create by chenqinping on 2019/4/4 16:11
 */
public class Table2TableApplication {

    public static void main(String[] args) throws Exception {

        //运行
        ToolRunner.run(new HbaseMapperReduceTool(), args);
    }
}
