package com.zhiyou100.mr.avroFile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.codec.digest.DigestUtils;
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

import com.zhiyou100.schema.SmallFile;

//计算合并文件后的大avro文件的词频
public class AvroFileMR {

	//读取avro文件, 没读取一条记录,
	//其实是一个小文件, 对其进行wordCount解析
	//并以(单词, 1)
	//的形式发送到reduce
	public static class AvroFileMRMap 
	extends Mapper<
	AvroKey<SmallFile>, NullWritable, 
	Text, IntWritable>{

		private Text outKey = 
				new Text();
		
		private final IntWritable ONE = 
				new IntWritable(1);
		
		private String[] infos;
		
		private ByteBuffer content;
		
		
		@Override
		protected void map(
				AvroKey<SmallFile> key, NullWritable value,
				Mapper<
				AvroKey<SmallFile>, NullWritable, 
				Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			content = key.datum().getContext();
			
			infos = new String(content.array()).split("\\s");
			
			for(String word : infos){
				outKey.set(word);
				context.write(outKey, ONE);
			}
		}
	}
	
	//把wordCount的计算结果, 以word_content.avro的
	//模式
	//输出成avro文件
	public static class AvroFileMRReduce 
	extends Reducer<
	Text, IntWritable, 
	AvroKey<GenericRecord>, NullWritable>{
		private int sum = 0;
		private Schema writeSchema;
		private GenericRecord record;
		
		private AvroKey<GenericRecord> outKey = 
				new AvroKey<GenericRecord>();	
		private NullWritable outValue =
				NullWritable.get();
		
		@Override
		protected void setup(Reducer<Text, IntWritable, AvroKey<GenericRecord>, NullWritable>.Context context)
				throws 
				IOException, 
				InterruptedException {
			Parser parser = 
					new Parser();
			
			writeSchema = 
					parser.parse(
							new File(
							"src/main/avro/word_count.avsc"));
			
			record = new GenericData.Record(writeSchema);
		}

		@Override
		protected void reduce(
				Text key, Iterable<IntWritable> values,
				
				Reducer<
				Text, IntWritable, 
				AvroKey<GenericRecord>, NullWritable
				>.Context context) 
						throws 
						IOException, 
						InterruptedException {
			
			sum = 0;
			
			for(IntWritable value : values){
				sum += value.get();
			}
			
			record.put("word", key);
			record.put("count", sum);
			
			outKey.datum(record);
			
			context.write(outKey, outValue);
		}
	}
	
	//读取daavro文件
	public static void readMergedFile(String avroFile) 
			throws IOException{
		
		DatumReader<SmallFile> reader = 
				new SpecificDatumReader<>();
		
		DataFileReader<SmallFile> fileReader = 
				new DataFileReader<>(
						new File(avroFile), reader);
		
		SmallFile smallFile = null;
		
		while(fileReader.hasNext()){
			
			smallFile = fileReader.next();
			
			System.out.println(
					"文件名:\t"
							+smallFile.getFileName());
			
			System.out.println(
					"文本内容:\t"+new String(
							smallFile.getContext().array()));
			//对照md5
			System.out.println(
					"文件md5:\t"
							+DigestUtils.md5Hex(
									smallFile
									.getContext()
									.array()));
			
		}
	}
	
	
	
	public static void main(String[] args) 
			throws 
			IOException, 
			ClassNotFoundException, 
			InterruptedException {
		
		Configuration conf = 
				new Configuration();
		Job job = Job.getInstance(
				conf, "计算合并文件后的大avro文件的词频");
		job.setJarByClass(AvroFileMR.class);
		
		
		job.setMapperClass(AvroFileMRMap.class);
		job.setReducerClass(AvroFileMRReduce.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(AvroKey.class);
		job.setOutputValueClass(NullWritable.class);
		
		//设置shuru输出的inputformat
		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		
		//设置输入输出的schema
		AvroJob.setInputKeySchema(
				job, SmallFile.getClassSchema());
		Parser parser = 
				new Parser();
		AvroJob.setOutputKeySchema(
				job, 
				parser.parse(
						new File(
								"src/main/avro/word_count.avsc"
								)));
		
		FileInputFormat.addInputPath(
				job, 
				new Path(
						"/user/reversetext1.avro"
						));
		
		Path outputDir = 
				new Path("/user/output/Avro/AvroFile");
		
		outputDir.getFileSystem(conf).delete(outputDir,true);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		/*AvroFile.readMergedFile(
				"C:\\Users\\Administrator\\"
				+ "Desktop/part-r-00000.avro");*/
	}
	
	
	
}
