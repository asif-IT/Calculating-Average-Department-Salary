package com.icelestio;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgSalMap extends Mapper<Text, Text, Text, IntWritable>{

	@Override
	protected void map(Text key, Text value, Context context)
		throws IOException, InterruptedException {
		String s = value.toString();
		String[] line = s.split("\t");
		Integer i = Integer.parseInt(line[1]);
		context.write(key, new IntWritable(i));
	}

}
