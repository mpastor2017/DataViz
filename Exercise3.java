package com.javamakeuse.hadoop.poc.Homework2;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Exercise3 extends Configured implements Tool {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text row_key = new Text();

		public void configure(JobConf job) {
		}

		protected void setup(OutputCollector<Text, Text> output)
				throws IOException, InterruptedException {
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String[] row = value.toString().split(",");

			String title = row[1];
			String artist = row[2];
			String dur = row[3];
			String year = row[165];

			if (year.matches("^\\d{4}")) {
				if (Integer.parseInt(year) <= 2010
						&& Integer.parseInt(year) >= 2000) {

					row_key.set(title + ", " + artist + ", " + dur);
					output.collect(row_key, new Text(""));
				}
			}
		}

		protected void cleanup(OutputCollector<Text, Text> output)
				throws IOException, InterruptedException {
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void configure(JobConf job) {
		}

		protected void setup(OutputCollector<Text, Text> output)
				throws IOException, InterruptedException {
		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

				output.collect(key, new Text(""));
		}

		protected void cleanup(OutputCollector<Text, Text> output)
				throws IOException, InterruptedException {
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), Exercise3.class);
		conf.setJobName("Exercise3");
				
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

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
		int res = ToolRunner.run(new Exercise3(), args);
		System.exit(res);
	}
}

