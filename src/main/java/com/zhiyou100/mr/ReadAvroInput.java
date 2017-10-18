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

	/**
	 * ReadAvroInputMap
	 */
	public static class ReadAvroInputMap extends Mapper<
	
	AvroKey<UserActionLog>, NullWritable, 
	Text, IntWritable> {

		private final IntWritable ONE = new IntWritable(1);

		private Text outKey = new Text();

		private UserActionLog keyData;

		@Override
		protected void map(

				AvroKey<UserActionLog> key, NullWritable value,

				Mapper<

						AvroKey<UserActionLog>, NullWritable, 
						Text, IntWritable>

				.Context context) throws IOException, InterruptedException {

			// 从封装类AvroKey, 从文件中将UserActionLog对象获取出来
			keyData = key.datum();

			outKey.set(keyData.getProvience().toString());

			// 少写了许多分析的代码
			context.write(outKey, ONE);
		}
	}

	/**
	 * ReadAvroInputReduce
	 */
	public static class ReadAvroInputReduce extends Reducer<
	
	Text, IntWritable, 
	Text, IntWritable> {

		private int sum;
		private IntWritable outValue = new IntWritable();

		@Override
		protected void reduce(
				
				Text key, Iterable<IntWritable> values,
				Reducer<
				Text, IntWritable, 
				Text, IntWritable>
				.Context context)
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
		Job job = Job.getInstance(conf, "读取avro文件进行每个省份访问用户数量计算");
		job.setJarByClass(ReadAvroInput.class);

		job.setMapperClass(ReadAvroInputMap.class);
		job.setReducerClass(ReadAvroInputReduce.class);

		// 设置文件的输入格式
		job.setInputFormatClass(AvroKeyInputFormat.class);

		// 设置输入avro文件的读取模式, 同时也可以使用下面的方法
		AvroJob.setInputKeySchema(job, UserActionLog.getClassSchema());

		// 设置输入key的schema
		// AvroJob.setInputKeySchema(job, UserActionLog.SCHEMA$);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path("/user/output/Avro/ReduceJoin"));
		Path outputDir = new Path("/user/output/Avro/ReadAvroInput");

		outputDir.getFileSystem(conf).delete(outputDir, true);
		FileOutputFormat.setOutputPath(job, outputDir);

		/*
		 * 首先他俩都是退出程序的意思：
		 *  
		 * 区别在于：
		 * system.exit（0）:正常退出，程序正常执行结束退出
		 * system.exit(1):是非正常退出，就是说无论程序正在执行与否，都退出，
		 * 
		 * System.exit(0)是将你的整个虚拟机里的内容都停掉了
		 * ，而dispose()只是关闭这个窗口，但是并没有停止整个application exit().
		 * 无论如何，内存都释放了！也就是说连JVM都关闭了，内存里根本不可能还有什么东西
		 * 
		 * System.exit(0)是正常退出程序，
		 * 而System.exit(1)或者说非0表示非正常退出程序
		 * 
		 * System.exit(status)不管status为何值都会退出程序。
		 * 和return相比有以下不同点：
		 * return是回到上一层，
		 * 而System.exit(status)是回到最上层
		 */
		
		//job.waitForCompletion(true)的值是true的时候
		//输出0, 正常退出
		//否则输出1, 非正常退出
		System.out.println(job.waitForCompletion(true)?0:1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}