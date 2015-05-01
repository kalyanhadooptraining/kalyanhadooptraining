package com.orienit.hadoop.training;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SedMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// read the line
		String line = value.toString();

		// check the line contains "hyderabad" key word or not
		if (line.contains("hyderabad")) {
			// replace "hyderabad" with "banglore"
			String replaceAll = line.replaceAll("hyderabad", "banglore");
			context.write(new Text(replaceAll), NullWritable.get());
		} else {
			context.write(value, NullWritable.get());
		}
	}

}
