package com.orienit.hadoop.training;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		// Initialize the sum with zero
		long sum = 0;

		// sum the list of values
		for (LongWritable value : values) {
			sum = sum + value.get();
		}

		// assign sum to corresponding word
		context.write(key, new LongWritable(sum));
	}
}
