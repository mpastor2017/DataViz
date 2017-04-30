package com.javamakeuse.hadoop.poc.Homework2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Exercise2 extends Configured implements Tool {

	// Map1 for 1-gram file
	public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		public void configure(JobConf job) {}

		protected void setup(OutputCollector<Text, Text> output)
				throws IOException, InterruptedException {}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
		
			String line = value.toString();
			String[] row = line.split("\\s");
			String vol = row[3];

			output.collect(new Text("key"), new Text(vol));
		}
		protected void cleanup(OutputCollector<Text, Text> output)
				throws IOException, InterruptedException {}
	}

	// Map2 for 2-gram file
	public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		public void configure(JobConf job) {}

		protected void setup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String[] row = line.split("\\s");

			// number of volumes
			String vol = row[4];
			output.collect(new Text("key"), new Text(vol));

		}
	protected void cleanup(OutputCollector<Text, Text> output)
			throws IOException, InterruptedException {}
	}
	
	public static class AvgCombiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            double sum = 0.0, count = 0.0, sumsq = 0.0;
	    
            //collect counter, sum of values, sum of squared values
	        while (values.hasNext()) {
				String vol = values.next().toString();
	        	count++;
				sum += Double.parseDouble(vol);
				sumsq +=  Double.parseDouble(vol)*Double.parseDouble(vol);
		    }
	        output.collect(key, new Text(Double.toString(count)+", "+Double.toString(sum)+", "+Double.toString(sumsq)));
        }
    }
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {

	public void configure(JobConf job) {}

	protected void setup(OutputCollector<Text, DoubleWritable> output)
			throws IOException, InterruptedException {}

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
        double sum = 0.0, count = 0.0, sumsq = 0.0;

		while (values.hasNext()) {
			Text pair = values.next();
			String[] tokens = pair.toString().split(",");
			
			//parse Text output from Combiner
			count += Double.parseDouble(tokens[0]);
	        sum += Double.parseDouble(tokens[1]);
	        sumsq += Double.parseDouble(tokens[2]);
		}
		
		//standard deviation in one pass
		double std = Math.sqrt((sumsq-sum*sum/count)/count);
		output.collect(key, new DoubleWritable(std));
	}

	protected void cleanup(OutputCollector<Text, DoubleWritable> output)
			throws IOException, InterruptedException {
	}
}
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(MultipleInputs.class);
		conf.setJobName("Exercise2");

	    conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(Text.class);
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(DoubleWritable.class);

	    conf.setCombinerClass(AvgCombiner.class);     
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setJarByClass(Exercise2.class);
		MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, Map1.class);
		MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, Map2.class);

		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Exercise2(), args);
		System.exit(res);
	}
}
