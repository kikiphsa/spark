package com.atguigu01.log;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Create by chenqingping on ${DATA}
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        boolean result = parseLog(line, context);

        //3清洗
        if (!result) {
            return;
        }
        k.set(line);
        // 4 输出合法的
        context.write(k, NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        boolean result;

        String[] fields = line.split(" ");

        if (fields.length > 11) {
            result = true;
//            context.getCounter("map", "parseLog_true").increment(1);
        } else {
            result = false;
//            context.getCounter("map", "parseLog_false").increment(1);
        }
        return result;
    }

}
