package com.youfan.bigdata.hbase.tool;

import com.youfan.bigdata.hbase.bean.CacheData;
import com.youfan.bigdata.hbase.format.MysqlOutputFormat;
import com.youfan.bigdata.hbase.mapper.ScanMysqlMapper;
import com.youfan.bigdata.hbase.reduce.ScanMysqlReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

/**
 * Create by chenqinping on 2019/4/8 18:31
 */
public class HBase2MysqlTool implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(HBase2MysqlTool.class);

        //mapper
        TableMapReduceUtil.initTableMapperJob(
                "atguigu:student1",
                new Scan(),
                ScanMysqlMapper.class,
                Text.class,
                CacheData.class,
                job);

        //reduce
        job.setReducerClass(ScanMysqlReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CacheData.class);

        //outputFormat
       job.setOutputFormatClass(MysqlOutputFormat.class);

        return job.waitForCompletion(true) ? JobStatus.State.SUCCEEDED.getValue():JobStatus.State.FAILED.getValue();
    }

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
