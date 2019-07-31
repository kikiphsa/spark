package com.atguigu01.dhfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;


/**
 * Create by chenqinping on 2019/3/1
 */
public class HdfsClient {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFs", "hdfs://hadoop102:9000");

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

        //创建目录
        fs.mkdirs(new Path("/user/atguigu/woinput"));

        fs.close();
    }
}
