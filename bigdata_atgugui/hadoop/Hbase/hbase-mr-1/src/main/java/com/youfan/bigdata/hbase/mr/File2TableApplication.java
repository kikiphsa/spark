package com.youfan.bigdata.hbase.mr;

import com.youfan.bigdata.hbase.mr.Tool.File2TableTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Create by chenqinping on 2019/4/8 11:12
 */
public class File2TableApplication {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new File2TableTool(),args);
    }
}
