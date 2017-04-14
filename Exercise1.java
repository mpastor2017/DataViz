package com.javamakeuse.hadoop.poc.homework1;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Exercise1 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	

	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    String line = value.toString();
	    String year = line.substring(15, 19);
	    int temp;
	    
	    if(line.charAt(87)=='+') {
	    	temp = Integer.parseInt(line.substring(88, 92));
	    } else {
	    	temp = Integer.parseInt(line.substring(87, 92));
	    }
	    
	    String quality = line.substring(92, 93);
	    
	    if(temp != 9999 && quality.matches("[01459]")) {
	    	output.collect(new Text(year), new IntWritable(temp));
	    }

	}

	protected void cleanup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
	}
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
	}

	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    int maxtemp = 0;
	    int currtemp;
	    while (values.hasNext()) {
	    	currtemp = values.next().get();
	    	if(currtemp > maxtemp) {
	    		maxtemp = currtemp;
	    	}
	    }
	    output.collect(key, new IntWritable(maxtemp));
	}

	protected void cleanup(OutputCollector<Text, IntWritable> output) throws IOException, InterruptedException {
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), Exercise1.class);
	conf.setJobName("exercise1");

	// conf.setNumReduceTasks(0);

	// conf.setBoolean("mapred.output.compress", true);
	// conf.setBoolean("mapred.compress.map.output", true);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);

	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new Exercise1(), args);
	System.exit(res);
    }
}
