package com.youfan.bigdata.hbase.mr.Tool;

import com.youfan.bigdata.hbase.mr.mapper.ReadFileMapper;
import com.youfan.bigdata.hbase.mr.reduce.InsertDataReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

/**
 * Create by chenqinping on 2019/4/8 11:13
 */
public class File2TableTool implements Tool {


    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(File2TableTool.class);

        //format
        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop102:9000/data/fruit.csv"));

        //mapper
        job.setMapperClass(ReadFileMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //reduce
        TableMapReduceUtil.initTableReducerJob("atguigu:student1", InsertDataReduce.class, job);


        return job.waitForCompletion(true) ? JobStatus.State.SUCCEEDED.getValue() : JobStatus.State.FAILED.getValue();
    }

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
