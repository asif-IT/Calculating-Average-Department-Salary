package com.icelestio;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Program extends Configured{

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job();
		
		job.setJarByClass(Program.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class); // Because we don't want the byte offset version.
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(AvgSalMap.class);
		job.setReducerClass(AvgSalReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0: -1);
	}

}
