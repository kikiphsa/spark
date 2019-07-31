package com.atguigu01.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderDriver {

	public static void main(String[] args) throws Exception, IOException {

		// 1 ��ȡ������Ϣ
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2 ����jar������·��
		job.setJarByClass(OrderDriver.class);

		// 3 ����map/reduce��
		job.setMapperClass(OrderMapper.class);
		job.setReducerClass(OrderReducer.class);

		// 4 ����map�������key��value����
		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);

		// 5 ��������������ݵ�key��value����
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);

		// 6 �����������ݺ��������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

//		// 10 ����reduce�˵ķ���
//		job.setGroupingComparatorClass(OrderGroupingComparator.class);

		// 7 ���÷���
		job.setPartitionerClass(OrderPartition.class);

		// 8 ����reduce����
		job.setNumReduceTasks(3);

		// 9 �ύ
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
