package com.youfan.bigdata.hbase.mapper;

import com.youfan.bigdata.hbase.bean.CacheData;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Create by chenqinping on 2019/4/8 18:36
 */
public class ScanMysqlMapper extends TableMapper<Text, CacheData> {

    @Override
    protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {

        for (Cell cell : result.rawCells()) {
            CacheData data = new CacheData();
            String name = Bytes.toString(CellUtil.cloneFamily(cell));
            data.setName(name);
            data.setCount(1);
            context.write(new Text(name), data);
        }

    }
}
