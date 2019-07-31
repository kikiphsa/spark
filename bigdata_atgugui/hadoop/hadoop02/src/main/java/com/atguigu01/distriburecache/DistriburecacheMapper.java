package com.atguigu01.distriburecache;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Create by chenqingping on ${DATA}
 */
public class DistriburecacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Map<String, String> map = new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(new File("pd.txt")), "UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            String[] fields = line.split("\t");
            map.put(fields[0],fields[1]);
        }
        // 关闭资源
        reader.close();
    }

    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1003	03	3
        String line = value.toString();
        String[] fields = line.split("\t");
        String pid = fields[1];
        String pName  = map.get(pid);
        k.set(line +"\t"+pName);
        context.write(k,NullWritable.get());
    }
}
