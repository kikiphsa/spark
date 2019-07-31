package com.youfan.bigdata.hbase.tool;

import com.youfan.bigdata.hbase.mapper.HadoopMapper;
import com.youfan.bigdata.hbase.reduce.HadoopReduce;
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
 * Create by chenqinping on 2019/4/8 19:23
 */
public class ToolHadoop implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(ToolHadoop.class);

        //format
        Path path = new Path("hdfs://hadoop102:9000/data/fruit.csv");
        FileInputFormat.addInputPath(job,path);
        //mapper
        job.setMapperClass( HadoopMapper.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //reduce
        TableMapReduceUtil.initTableReducerJob(
                "atguigu:student1",
                HadoopReduce.class,
                job
        );


        return job.waitForCompletion(true)? JobStatus.State.SUCCEEDED.getValue():JobStatus.State.FAILED.getValue();
    }

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
