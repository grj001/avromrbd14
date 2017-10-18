package com.zhiyou100.mr;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zhiyou100.schema.UserActionLog;

//计算每个省份的用户对系统的访问
public class ReadAvroInput {

	public static class ReadAvroInputMap 
	extends Mapper<
			AvroKey<UserActionLog>, NullWritable, 
			Text, 					IntWritable
		> {

		private final IntWritable ONE = new IntWritable(1);

		private Text outKey = new Text();

		private UserActionLog keyData;

		@Override
		protected void map(AvroKey<UserActionLog> key, NullWritable value,
				Mapper<AvroKey<UserActionLog>, NullWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			// chong封装类AvroKey把UserActionLog对象获取出来
			keyData = key.datum();

			outKey.set(keyData.getProvience().toString());

			// 少写了许多解析的代码
			context.write(outKey, ONE);

		}

	}

	public static class ReadAvroInputReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		private int sum;
		private IntWritable outValue = new IntWritable();

		@Override
		protected void reduce(
				
				Text key, Iterable<IntWritable> values,
				Reducer<
				
				Text, IntWritable, 
				Text, IntWritable
				
				>.Context context)
				throws IOException, InterruptedException {
			
			sum = 0;
			for (IntWritable value : values) {
				
				sum += value.get();
			}

			outValue.set(sum);
			context.write(key, outValue);
		}
	}

	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "读取avro文件进行每日计算");
		job.setJarByClass(ReadAvroInput.class);
		
		job.setMapperClass(ReadAvroInputMap.class);
		job.setReducerClass(ReadAvroInputReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//设置文件输入格式
		job.setInputFormatClass(AvroKeyInputFormat.class);
		
		//设置输入avro文件的读取模式, 同时也可以使用下面的方法
		AvroJob.setInputKeySchema(job, UserActionLog.getClassSchema());
		
		//设置输出key的schema
		//AvroJob.setIntputKeySchema(job, UserActionLog.SCHEMA$);
		
		
		
		//user_info.txt
		FileInputFormat.addInputPath(job, new Path("/user/output/Avro/ReduceJoin"));
		Path outputDir = new Path("/user/output/Avro/ReadAvroInput");
		outputDir.getFileSystem(conf).delete(outputDir,true);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
