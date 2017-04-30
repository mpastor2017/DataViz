package com.javamakeuse.hadoop.poc.Homework2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Exercise4 extends Configured implements Tool {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		private Text row_key = new Text();

		public void configure(JobConf job) {
		}

		protected void setup(OutputCollector<Text, DoubleWritable> output)
				throws IOException, InterruptedException {
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] row = line.split(",");

			String artist = row[2];
			String dur = row[3];

			// capitalize first letter for sorting
			artist = artist.substring(0, 1).toUpperCase() + artist.substring(1);

			row_key.set(artist);
			output.collect(row_key, new DoubleWritable(Double.parseDouble(dur)));
		}

		protected void cleanup(OutputCollector<Text, DoubleWritable> output)
				throws IOException, InterruptedException {
		}
	}

	public static class Partition implements Partitioner<Text, DoubleWritable> {

		public int getPartition(Text key, DoubleWritable value,
				int numReduceTasks) {
			String strkey = key.toString().toLowerCase();
			if (numReduceTasks == 0) {
				return 0;
			}
			if (strkey.compareTo("e") < 0) {
				return 0;
			} else if (strkey.compareTo("j") < 0) {
				return 1 % numReduceTasks;
			} else if (strkey.compareTo("o") < 0) {
				return 2 % numReduceTasks;
			} else if (strkey.compareTo("t") < 0) {
				return 3 % numReduceTasks;
			} else {
				return 4 % numReduceTasks;
			}
		}

		public void configure(JobConf arg0) {
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void configure(JobConf job) {
		}

		protected void setup(OutputCollector<Text, DoubleWritable> output)
				throws IOException, InterruptedException {
		}

		public void reduce(Text key, Iterator<DoubleWritable> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			double max = -1.0;

			while (values.hasNext()) {
				double dur = values.next().get();

				if (dur > max) {
					max = dur;
				}
			}
			output.collect(key, new DoubleWritable(max));
		}

		protected void cleanup(OutputCollector<Text, DoubleWritable> output)
				throws IOException, InterruptedException {
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), Exercise4.class);

		conf.setJobName("Exercise4");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(DoubleWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setPartitionerClass(Partition.class);
		conf.setReducerClass(Reduce.class);

		conf.setNumReduceTasks(5);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Exercise4(), args);
		System.exit(res);
	}
}
