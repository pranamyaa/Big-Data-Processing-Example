package edu.rmit.cosc2367.s3779009.Assignment1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;



public class Task3 {
	private static final Logger LOG = Logger.getLogger(Task3.class);

	public static class TokenizerMapper extends Mapper <Object, Text, Text, IntWritable> {
		private Text word = new Text(); 
		private Map <String, Integer> combine = new HashMap <String, Integer>();
		// Mapper Function for task 3 with in-mapper combining functionality
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			LOG.info("The Mapper Task of Pranamya K, S3779009 of Task3 Assignment 1");
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String singleword = itr.nextToken();
				if(combine.containsKey(singleword)) {
					combine.put(singleword, combine.get(singleword)+1);
				}
				else {
					combine.put(singleword, 1);
				}
			}
			for(Map.Entry<String, Integer> set : combine.entrySet()) {
				word.set(set.getKey());
				IntWritable Value = new IntWritable(set.getValue());
				context.write(word, Value);
			}
		}
		
	}
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		//Reducer function to reuce and generate output.
		public void reduce(Text key, Iterable<IntWritable> values,	Context context) throws IOException, InterruptedException
		{
			LOG.info("The Reducer Task of Pranamya K, S3779009 of Task3 Assignment 1");
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		LOG.setLevel(Level.DEBUG);
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Inmapper Combining");
		job.setJarByClass(Task3.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}
