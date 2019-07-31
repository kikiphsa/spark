package com.atguigu01.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

// k atguigu--a.txt
// v  ����
public class OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	String name;
	Text k = new Text();
	IntWritable v = new IntWritable(1);
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		// ��ȡ����
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		name = inputSplit.getPath().getName();

	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		// 1 ��ȡһ��
		// atguigu pingping
		String line = value.toString();

		// 2 �и�
		// atguigu
		// pingping
		String[] words = line.split(" ");

		// 3 �������
		for (String word : words) {
			// atguigu--a.txt
			k.set(word + "--" + name);

			context.write(k, v);
		}

	}

}
