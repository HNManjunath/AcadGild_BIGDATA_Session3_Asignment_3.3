package com.tvbrand;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MainMethod {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		Job job = new Job();								
		job.setJarByClass(MainMethod.class);				
		job.setJobName("count no of units");					
		
		job.setMapperClass(MapperClass.class);				//	Setting the Mapper Class
		job.setReducerClass(Task1Reducer.class); 			//	Setting the Reducer Class

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); //File input path
		FileOutputFormat.setOutputPath(job,new Path(args[1]));//File output path
		
		job.waitForCompletion(true);
		
		
		Job secondjob = new Job();								
		secondjob.setJarByClass(MainMethod.class);				
		secondjob.setJobName("count particular brand unit");					
		
		secondjob.setMapperClass(MapperClass.class);				//	Setting the same Mapper Class to get input
		secondjob.setReducerClass(Task2Reducer.class); 			//	Setting the second Reducer Class

		secondjob.setOutputKeyClass(Text.class);
		secondjob.setOutputValueClass(Text.class);
		 
		secondjob.setInputFormatClass(TextInputFormat.class);
		secondjob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(secondjob, new Path(args[0])); 			//File input path
		FileOutputFormat.setOutputPath(secondjob,new Path(args[1]+"secondjob"));	//File output path
		
		secondjob.waitForCompletion(true);
	}
}
