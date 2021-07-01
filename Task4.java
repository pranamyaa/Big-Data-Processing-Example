package edu.rmit.cosc2367.s3779009.Assignment1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class Task4 {
	private static final Logger LOG = Logger.getLogger(Task3.class);
	
	public static class TokenizerMapper extends Mapper <Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		// Mapper Function for task 4
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			LOG.info("The Mapper Task of Pranamya K, S3779009 of Task4 Assignment 1");
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String Word = itr.nextToken();
				String result = findLength(Word);
				word.set(result);
				context.write(word, one);
			}
		}
		private String findLength(String Word) {
			int length = Word.length();
			if (length <= 4) {
				return "Short Word";
			}
			else if(length >=5 && length <= 7) {
				return "Medium Word";
			}
			else if(length >= 8 && length <= 10) {
				return "Long Word";
			}
			else if(length > 10) {
				return "ExtraLong Word";
			}
			else {
				return null;
			}
		}
		
	}
	
	
	//// Partitioner for Task 4
	public static class TokenizerPartitioner extends Partitioner<Text, IntWritable>{

		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			// TODO Auto-generated method stub
			 if(numPartitions == 2){
	             String Keylength = key.toString();
	             if(Keylength.equals("Short Word") || Keylength.equals("ExtraLong Word"))
	                 return 0;
	             else 
	                 return 1;
	         } else if(numPartitions == 1)
	             return 0;
	         else{
	             System.err.println("TokenizerParitioner can only handle either 1 or 2 paritions");
	             return 0;
	         }
		}
		
	}
	
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		
		// Reducer to reduce and generate final output
		public void reduce(Text key, Iterable<IntWritable> values,	Context context) throws IOException, InterruptedException
		{
			LOG.info("The Reducer Task of Pranamya K, S3779009 of Task4 Assignment 1");
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
		Job job = Job.getInstance(conf, "word lengths with Partitioner");
		job.setJarByClass(Task4.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setPartitionerClass(TokenizerPartitioner.class);
		job.setNumReduceTasks(2);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
