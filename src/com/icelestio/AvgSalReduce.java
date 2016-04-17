package com.icelestio;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgSalReduce extends Reducer<Text, IntWritable, Text, DoubleWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
		int count = 0, sum = 0;
		double average = 0.0;
		
		while(values.iterator().hasNext()){
			int salary = values.iterator().next().get(); // next() returns IntWritable which returns int from get().
			sum += salary;
			++count;
		}
		
		average = (double)sum/count;
		context.write(key, new DoubleWritable(average));
	}
	
}
