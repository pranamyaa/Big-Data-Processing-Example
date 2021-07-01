package edu.rmit.cosc2367.s3779009.Assignment1;

import java.io.IOException;
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



public class Task2 {
	private static final Logger LOG = Logger.getLogger(Task2.class);

	public static class TokenizerMapper extends Mapper <Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		// Mapper Function for task 2 which calls method findStart() to check if the word starts with vowel or consonant
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			LOG.info("The Mapper Task of Pranamya K, S3779009 of Task2 Assignment 1");
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String Word = itr.nextToken();
				String result = findStart(Word);
				word.set(result);
				context.write(word, one);
			}
		}
		private String findStart(String Word) {
			String lower = Word.toLowerCase();
			char start = lower.charAt(0);
			if(start == 'a' || start == 'e' || start == 'i' || start == 'o' || start == 'u') {
				return "Vowel";
			}
			else {
				return "Consonant";
			}
		}
		
	}
	
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		//Reducer function to reduce and generate output.
		public void reduce(Text key, Iterable<IntWritable> values,	Context context) throws IOException, InterruptedException
		{
			LOG.info("The Reducer Task of Pranamya K, S3779009 of Task1 Assignment 1");
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
		Job job = Job.getInstance(conf, "vowels and consonants");
		job.setJarByClass(Task2.class);
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
