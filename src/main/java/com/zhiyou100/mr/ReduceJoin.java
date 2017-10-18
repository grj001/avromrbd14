package com.zhiyou100.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zhiyou100.schema.UserActionLog;


public class ReduceJoin {
	
	//ValueWithFlag
	public static class ValueWithFlag implements Writable{
		private String value;
		private String flag;
		public String getValue() {
			return value;
		}
		public void setValue(String value) {
			this.value = value;
		}
		public String getFlag() {
			return flag;
		}
		public void setFlag(String flag) {
			this.flag = flag;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(value);
			out.writeUTF(flag);
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			this.value = in.readUTF();
			this.flag = in.readUTF();
		}
		@Override
		public String toString() {
			return "ValueWithFlag [value=" + value + ", flag=" + flag + "]";
		}
	}
	
	//ReduceJoinMap
	public static class ReduceJoinMap extends Mapper<
	
		LongWritable, Text, 
		Text, ValueWithFlag>{
		
		private FileSplit inputSplit;
		private String fileName;
		private String[] infos;
		private Text outKey = new Text();
		private ValueWithFlag outValue = new ValueWithFlag();
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, ValueWithFlag>.Context context)
				throws IOException, InterruptedException {
			
			inputSplit = (FileSplit) context.getInputSplit();
			
			
			if(inputSplit.getPath().toString().contains("user-logs-large.txt")){
				fileName = "userLogsLarge";
			}else if(inputSplit.getPath().toString().contains("user_info.txt")){
				fileName = "userinfo";
			}
			
		}
		
		@Override
		protected void map(
				LongWritable key, Text value,
				
				Mapper<
				LongWritable, Text, 
				Text, ValueWithFlag>.Context context)
		
				throws IOException, InterruptedException {
			
			
			infos = value.toString().split("\\s");
			
			if(fileName.equals("userLogsLarge")){
				
				outKey.set(infos[0]);
				outValue.setValue(infos[1]+"\t"+infos[2]);
				
			}else if(fileName.equals("userinfo")){
				
				outKey.set(infos[0]); // 0:mike 1:man 2:henan
				outValue.setValue(infos[1]+"\t"+infos[2]); // 0:mike 1:man 2:henan
				
			}
			
			outValue.setFlag(fileName);
			context.write(outKey, outValue);
		}
	}
	
	//ReduceJoinReduce
	public static class ReduceJoinReduce 
		extends Reducer<
		
			Text, ValueWithFlag, 
			AvroKey<UserActionLog>, NullWritable
		>{
		
		private List<String> userLogsLargeList;
		private List<String> userInfoList;
		
		
		private AvroKey<UserActionLog> outKey = 
				new AvroKey<UserActionLog>();
		private NullWritable outValue = 
				NullWritable.get();
		
		
		private String[] infos;
		
		
		@Override
		protected void reduce(
				
				Text key, Iterable<ValueWithFlag> values,
				
				Reducer<
				
				Text, ValueWithFlag, 
				AvroKey<UserActionLog>, NullWritable
				
				>.Context context) throws IOException, InterruptedException {
			
			
			userLogsLargeList = new ArrayList<String>();
			userInfoList = new ArrayList<String>();
			
			
			for(ValueWithFlag value : values){
				
				
				if(value.getFlag().equals("userLogsLarge")){
					userLogsLargeList.add(value.getValue());
				}else if(value.getFlag().equals("userinfo")){
					userInfoList.add(value.getValue());
				}
				
				
			}
			
			
			//对应同一个key:用户名	对两组的数据进行笛卡尔乘积
			for(String userLogsLarge : userLogsLargeList){
				
				for(String userInfo : userInfoList){
					//构建userActionLog对象
					UserActionLog.Builder builder = 
							UserActionLog
							.newBuilder();
					
					
					//重userlogslarge中提取actiontype和ipaddress
					infos = userLogsLarge.split("\\s");
					builder.setActionType(infos[0]);
					builder.setIpAddress(infos[1]);
					
					
					//重userinfo中提取gender和省份
					infos = userInfo.split("\\s");
					if(infos[0].equals("man")){
						builder.setGender(1);
					}else {
						builder.setGender(0);
					}
					
					
					builder.setProvience(infos[1]);
					
					
					//重key中图区username
					builder.setUserName(key.toString());
					
					
					UserActionLog userActionLog = builder.build();
					
					
					//吧userActionLog封装到AvroKey中
					outKey.datum(userActionLog);

					
					context.write(outKey, outValue);
					
				}
			}
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "reduce端关联");
		job.setJarByClass(ReduceJoin.class);
		
		job.setMapperClass(ReduceJoinMap.class);
		job.setReducerClass(ReduceJoinReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ValueWithFlag.class);
		job.setOutputKeyClass(AvroKey.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		//设置输出的各司是avroKey
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		
		//设置输出key的schema
		AvroJob.setOutputKeySchema(job, UserActionLog.SCHEMA$);
		
		
		
		
		
		
		
		//user_info.txt
		FileInputFormat.addInputPath(job, new Path("/user/user_info.txt"));
		FileInputFormat.addInputPath(job, new Path("/user/user-logs-large.txt"));
		Path outputDir = new Path("/user/output/Avro/ReduceJoin");
		outputDir.getFileSystem(conf).delete(outputDir,true);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
