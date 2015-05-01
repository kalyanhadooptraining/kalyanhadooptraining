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

public class SedJob implements Tool {
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
		Job sedJob = new Job(getConf());

		// setting the job name
		sedJob.setJobName("Orien IT Sed Job");

		// to call this as a jar
		sedJob.setJarByClass(this.getClass());

		// setting custom mapper class
		sedJob.setMapperClass(SedMapper.class);

		// setting custom reducer class
//		grepJob.setReducerClass(WordCountReducer.class);

		// setting custom combiner class
//		grepJob.setCombinerClass(WordCountReducer.class);

		// setting no of reducers
		sedJob.setNumReduceTasks(0);

		// setting custom partitioner class
//		grepJob.setPartitionerClass(WordCountPartitioner.class);

		// setting mapper output key class: K2
		sedJob.setMapOutputKeyClass(Text.class);

		// setting mapper output value class: V2
		sedJob.setMapOutputValueClass(NullWritable.class);

		// setting reducer output key class: K3
		sedJob.setOutputKeyClass(Text.class);

		// setting reducer output value class: V3
		sedJob.setOutputValueClass(NullWritable.class);

		// setting the input format class ,i.e for K1, V1
		sedJob.setInputFormatClass(TextInputFormat.class);

		// setting the output format class
		sedJob.setOutputFormatClass(TextOutputFormat.class);

		// setting the input file path
		FileInputFormat.addInputPath(sedJob, new Path(args[0]));

		// setting the output folder path
		FileOutputFormat.setOutputPath(sedJob, new Path(args[1]));

		Path outputpath = new Path(args[1]);
		// delete the output folder if exists
		outputpath.getFileSystem(conf).delete(outputpath, true);

		// to execute the job and return the status
		return sedJob.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		// start the job providing arguments and configurations
		int status = ToolRunner.run(new Configuration(), new SedJob(),
				args);
		System.out.println("My Status: " + status);
	}

}
















