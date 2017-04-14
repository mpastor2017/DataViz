package com.javamakeuse.hadoop.poc.hw1prob2;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Exercise2 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	private final static DoubleWritable one = new DoubleWritable(1);
	

	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    String line = value.toString();
	    String linekey;
	    double fourthval;
	    if(!Boolean.parseBoolean(line.split(",")[line.split(",").length-1].toString())) {
	    	linekey = line.split(",")[29].charAt(0) + "," + line.split(",")[30].charAt(0) + "," + line.split(",")[31].charAt(0) + "," + line.split(",")[32].charAt(0);
	    	fourthval = Double.parseDouble(line.split(",")[3]);
	    	output.collect(new Text(linekey), new DoubleWritable(fourthval));
	    }
	}

	    

	protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
	}
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
	}

	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    double avg = 0;
	    int keysize = 0;
	    while (values.hasNext()) {
	    	avg += values.next().get();
	    	keysize += 1;
	    }
	    avg = avg / keysize;
	    output.collect(key, new DoubleWritable(avg));
	}

	protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), Exercise2.class);
	conf.setJobName("exercise2");

	// conf.setNumReduceTasks(0);

	// conf.setBoolean("mapred.output.compress", true);
	// conf.setBoolean("mapred.compress.map.output", true);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(DoubleWritable.class);

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
	int res = ToolRunner.run(new Configuration(), new Exercise2(), args);
	System.exit(res);
    }
}
