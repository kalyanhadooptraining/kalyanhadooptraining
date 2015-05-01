package com.orienit.hadoop.training;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GrepJob implements Tool {
	// intializing configuration object
	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf; // getting the configuration
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf; // setting the configuration
	}

	@Override
	public int run(String[] args) throws Exception {

		// initializing the job configuration
		Job grepJob = new Job(getConf());

		// setting the job name
		grepJob.setJobName("Orien IT Grep Job");

		// to call this as a jar
		grepJob.setJarByClass(this.getClass());

		// setting custom mapper class
		grepJob.setMapperClass(GrepMapper.class);

		// setting custom reducer class
//		grepJob.setReducerClass(WordCountReducer.class);

		// setting custom combiner class
//		grepJob.setCombinerClass(WordCountReducer.class);

		// setting no of reducers
		grepJob.setNumReduceTasks(0);

		// setting custom partitioner class
//		grepJob.setPartitionerClass(WordCountPartitioner.class);

		// setting mapper output key class: K2
		grepJob.setMapOutputKeyClass(Text.class);

		// setting mapper output value class: V2
		grepJob.setMapOutputValueClass(NullWritable.class);

		// setting reducer output key class: K3
		grepJob.setOutputKeyClass(Text.class);

		// setting reducer output value class: V3
		grepJob.setOutputValueClass(NullWritable.class);

		// setting the input format class ,i.e for K1, V1
		grepJob.setInputFormatClass(TextInputFormat.class);

		// setting the output format class
		grepJob.setOutputFormatClass(TextOutputFormat.class);

		String multipath = args[0];
		for (int i = 1; i < args.length - 1; i++) {
			multipath = multipath + "," + args[i];
		}
		String output = args[args.length - 1];
		// setting the input file path
		// FileInputFormat.addInputPath(grepJob, new Path(args[0]));
		FileInputFormat.addInputPaths(grepJob, multipath);

		// setting the output folder path
		// FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(grepJob, new Path(output));

		Path outputpath = new Path(output);
		// delete the output folder if exists
		outputpath.getFileSystem(conf).delete(outputpath, true);

		// to execute the job and return the status
		return grepJob.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		// start the job providing arguments and configurations
		int status = ToolRunner.run(new Configuration(), new GrepJob(),
				args);
		System.out.println("My Status: " + status);
	}

}
















