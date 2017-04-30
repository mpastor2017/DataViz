package com.javamakeuse.hadoop.poc.Homework2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Exercise1 extends Configured implements Tool {

	// Map1 for 1-gram file
	public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		private Text row_key = new Text();

		public void configure(JobConf job) {}

		protected void setup(OutputCollector<Text, Text> output)
				throws IOException, InterruptedException {}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
			String line = value.toString();
			String[] row = line.split("\\s");
			
			String word1 = row[0].toLowerCase().trim();
			String year = row[1].trim();
			String vol = row[3].trim();
			
			if (year.matches("\\d{4}")) {
				if (word1.contains("nu")) {
					row_key.set(year + ", " + "nu");
					output.collect(row_key, new Text(vol));
				}
				if (word1.contains("chi")) {
					row_key.set(year + ", " + "chi");
					output.collect(row_key, new Text(vol));
				}
				if (word1.contains("haw")) {
					row_key.set(year + ", " + "haw");
					output.collect(row_key, new Text(vol));
				}
			}
		}
		protected void cleanup(OutputCollector<Text, Text> output)
				throws IOException, InterruptedException {}
	}

	// Map2 for 2-gram file
	public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		private Text row_key = new Text();

		public void configure(JobConf job) {}

		protected void setup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String[] row = line.split("\\s");

			String word1 = row[0].toLowerCase();
			String word2 = row[1].toLowerCase();
			String year = row[2];
			String vol = row[4];
			
			if (year.matches("^\\d{4}")) {
				if (word1.contains("nu")) {
					row_key.set(year + ", " + "nu");
					output.collect(row_key, new Text(vol));
				}
				if (word1.contains("chi")) {
					row_key.set(year + ", " + "chi");
					output.collect(row_key, new Text(vol));
				}
				if (word1.contains("haw")) {
					row_key.set(year + ", " + "haw");
					output.collect(row_key, new Text(vol));
				}
				if (word2.contains("nu")) {
					row_key.set(year + ", " + "nu");
					output.collect(row_key, new Text(vol));
				}
				if (word2.contains("chi")) {
					row_key.set(year + ", " + "chi");
					output.collect(row_key, new Text(vol));
				}
				if (word2.contains("haw")) {
					row_key.set(year + ", " + "haw");
					output.collect(row_key, new Text(vol));
				}
			}
		}
		
	protected void cleanup(OutputCollector<Text, Text> output)
			throws IOException, InterruptedException {}
	}
	
	public static class AvgCombiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            double sum = 0.0, count = 0.0;
	    
	        while (values.hasNext()) {
	        	count++;
				sum += Double.parseDouble(values.next().toString());
		    }
		 
	        output.collect(key, new Text(Double.toString(sum)+", "+Double.toString(count)));
        }
    }
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {

	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, DoubleWritable> output)
			throws IOException, InterruptedException {
	}

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
		double sum = 0.0;
		double size = 0.0;

		while (values.hasNext()) {
			Text pair = values.next();
			String[] tokens = pair.toString().split(",");
			size = size + Double.parseDouble(tokens[1]);
	        sum += Double.parseDouble(tokens[0]); 
		}
		double avg = sum/size;
		output.collect(key, new DoubleWritable(avg));
	}

	protected void cleanup(OutputCollector<Text, DoubleWritable> output)
			throws IOException, InterruptedException {
	}
}
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(MultipleInputs.class);
		conf.setJobName("Exercise1");

	    conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(Text.class);
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(DoubleWritable.class);

	    conf.setCombinerClass(AvgCombiner.class);     
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		//added
		conf.setJarByClass(Exercise1.class);
		MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, Map1.class);
		MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, Map2.class);

		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Exercise1(), args);
		System.exit(res);
	}
}
