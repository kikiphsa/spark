package com.atguigu01.dhfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * Create by chenqinping on 2019/3/1
 */
public class testCopyFromLocalFile {

    @Test
    public void testCopyFromLocalFile1() throws Exception {
        Configuration configuration = new Configuration();

        configuration.set("dfs.replication", "2");

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

        //上传文件
        fs.copyFromLocalFile(new Path("D:\\ysyys.txt"), new Path("/yxyyx.ttx"));

        fs.close();
        System.out.println("over");
    }

    @Test
    public void copyFile() throws Exception {

        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

        fs.copyToLocalFile(false, new Path("/yxyyx.ttx"), new Path("D:/jjj.txt"), true);

        fs.close();
    }


    @Test
    public void del() throws Exception {

        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

        fs.delete(new Path("/yxyyx.ttx"), true);

        fs.close();
    }


    @Test
    public void putFileToHDFS()throws Exception{

        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

        FileInputStream fis = new FileInputStream(new File("d://jjj.txt"));

        FSDataOutputStream fos = fs.create(new Path("/jjj.txt"));

        IOUtils.copyBytes(fis,fos,configuration);

        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    @Test
    public void putFileToHDFS1()throws Exception{

        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

        FSDataInputStream fis = fs.open(new Path("/jjj.txt"));
        FileOutputStream fos = new FileOutputStream(new File("e:/banhua.txt"));


        IOUtils.copyBytes(fis,fos,configuration);

        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }
}
