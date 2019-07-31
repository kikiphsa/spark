package com.youfan.bigdata.hbase.reduce;

import com.youfan.bigdata.hbase.bean.CacheData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/4/8 18:41
 */
public class ScanMysqlReduce extends Reducer<Text, CacheData, Text, CacheData> {

    @Override
    protected void reduce(Text key, Iterable<CacheData> values, Context context) throws IOException, InterruptedException {

        int sumcount=0;

        for (CacheData data : values) {
            sumcount += data.getCount();
        }

        CacheData data = new CacheData();
        data.setName(key.toString());
        data.setCount(sumcount);

        context.write(key,data);
    }
}
