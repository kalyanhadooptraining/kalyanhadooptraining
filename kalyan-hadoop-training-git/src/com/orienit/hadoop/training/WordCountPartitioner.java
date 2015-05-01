package com.orienit.hadoop.training;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, LongWritable> {

	@Override
	public int getPartition(Text key, LongWritable value, int noOfReducers) {
		// convert word into lower case
		String word = key.toString().toLowerCase();

		// if empty line return to partition zero
		if (word.length() == 0)
			return 0;

		// read the first char
		char firstChar = word.charAt(0);

		// get proper partition
		int diff = Math.abs(firstChar - 'a') % noOfReducers;

		// return the partition number
		return diff;
	}

}
