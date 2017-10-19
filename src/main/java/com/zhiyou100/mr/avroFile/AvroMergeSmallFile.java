package com.zhiyou100.mr.avroFile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;

import com.zhiyou100.schema.SmallFile;

//把文件夹下的小文件合并成为大文件
public class AvroMergeSmallFile {

	private Schema.Parser parser = new Schema.Parser();
	
	private Schema schema;
	
	//用来设施要被合并的文件的名称
	private List<String> inputFilePaths = new ArrayList<>();
	
	//在构造方法初始化schema
	public AvroMergeSmallFile(){
		//schema包下的SmallFile约束
		schema = SmallFile.getClassSchema();
	}
	
	//添加要被合并的文件夹
	public void addInputFileDir(String inputDir) throws Exception{
		
		//获取文件夹下的所有文件
		File[] files = FileUtil.listFiles(new File(inputDir));
		
		//把文件路径添加到inputFilePaths中
		for(File file : files){
			inputFilePaths.add(file.getPath());
		}
	}
	
	
	//把inputFilePaths中的所有文件合并到一个avro文件中
	public void mergeFile(String outputPath) 
			throws Exception{
		
		//创建DatumWriter<SmallFile>对象 	writer
		DatumWriter<SmallFile> writer =  
				new SpecificDatumWriter<SmallFile>();
		
		//创建DataFileWriter<SmallFile>对象    fileWriter
		DataFileWriter<SmallFile> fileWriter = 
				new DataFileWriter<SmallFile>(writer);
		
		//生成输出文件的目录, 创建DataFileWriter<SmallFile>对象 create
		DataFileWriter<SmallFile> create = 
				fileWriter.create(schema, new File(outputPath));
		
		/*
		 * 1. 遍历存在的需要合并的文件路径
		 * 2. 在for循环中
		 * 		将文件内容存入字节数组中
		 * 		创建一个avro对象, 设置文件名称和文件内容
		 * 		将创建的对象写入到文件中
		 */
		for(String filePath : inputFilePaths){
			
			File inputFile = new File(filePath);
			
			//把文件读取成字符串
			byte[] content = 
					FileUtils.readFileToByteArray(inputFile);
			
			//ByteBuffer调用wrap方法把字符数组封装成一个byteBuffer对象
			//作为参数设置到oneSmallFile的content属性中
			SmallFile oneSmallFile = 
					SmallFile
					.newBuilder()
					.setFileName(inputFile.getAbsolutePath())
					.setContext(ByteBuffer.wrap(content))
					.build();
			
			//将oneSmallFile对象写入到文件中去
			fileWriter.append(oneSmallFile);
			System.out.println("写入"+inputFile.getAbsolutePath()
					+"成功"
					//这个方法计算一个数据的md5值
					+DigestUtils.md5Hex(content));
			
		}
		
		fileWriter.flush();
		fileWriter.close();
	}
	
	
	
	//读取到avro文件
	public void readMergedFile(String avroFile) 
			throws IOException{
		
		DatumReader<SmallFile> reader = 
				new SpecificDatumReader<SmallFile>();
		
		DataFileReader<SmallFile> fileReader = 
				new DataFileReader<SmallFile>
					(new File(avroFile), reader);
		
		SmallFile smallFile = null;
		
		while(fileReader.hasNext()){
			 
			smallFile = fileReader.next();
			
			System.out.println(
						"文件名:\t"
						+smallFile.getFileName());
			
			System.out.println(
						"文本内容:\t"
						+new String(
								smallFile
								.getContext().array())
					);
			
			System.out.println(
						"文件md5:\t"
						+DigestUtils.md5Hex(
								smallFile
								.getContext()
								.array()
								)
					);
			
		}
	}
	
	
	
	
	
	public static void main(String[] args) throws Exception{
		
		//创建这个类的对象, 就能调用其相应的方法
		AvroMergeSmallFile avroMergeSmallFile = 
				new AvroMergeSmallFile();
		
		avroMergeSmallFile.addInputFileDir("D:\\test\\reversetext");
		
		avroMergeSmallFile.mergeFile(
				"D:\\test\\reversetext1.avro"
				);
		
		avroMergeSmallFile.readMergedFile(
				"D:\\test\\reversetext1.avro"
				);
	}
	
	
}