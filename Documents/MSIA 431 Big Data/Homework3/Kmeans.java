package com.javamakeuse.hadoop.poc.Homework3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Kmeans {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public static ArrayList<double[]> centers = new ArrayList<double[]>();

		/*
		 * Read in centroids.txt file
		 */
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {

				BufferedReader cacheReader = new BufferedReader(new FileReader("centroids.txt"));

				String line;
				while ((line = cacheReader.readLine()) != null) {

					String row[] = line.split("\\s+");
					
					//add values to list for each line (centroid)
					double[] column_val = new double[row.length];
					
					for (int i = 0; i < row.length; i++) {
						column_val[i] = Double.parseDouble(row[i]);
					}
					centers.add(column_val);
				}
				cacheReader.close();
			}
			super.setup(context);
		}
		
		/*
		 * Calculate distance from row to each centroid and collect index of nearest centroid
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] row = line.trim().split("\\s+");

			ArrayList<Double> dist_list = new ArrayList<Double>();

			double d = Integer.MAX_VALUE;
			for (int i = 0; i < centers.size(); i++) {
				d = distance(row, centers.get(i));
				dist_list.add(d);
			}

			int min = dist_list.indexOf(Collections.min(dist_list));
			value.set(line);

			context.write(new Text(Integer.toString(min)), value);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

		Text newCentroids = new Text();

		/*
		 * Collect all points nearest each centroid and calculate centroid of these points
		 */
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int count = 0;
			ArrayList<Double> sum = new ArrayList<Double>();
			String newCentroid = "";
			for (Text val : values) {
				String[] columns = val.toString().trim().split("\\s+");
				count++;
				for (int i = 0; i < columns.length; i++) {
					if (sum.size() == columns.length) {
						sum.set(i, sum.get(i) + Double.parseDouble(columns[i]));
					}
					//element is empty for first row
					else {
						sum.add(Double.parseDouble(columns[i]));
					}
				}
			}
			//calculate column means
			for (int i = 0; i < sum.size(); i++) {
				newCentroid = newCentroid + (sum.get(i) / count) + " ";
			}
			context.write(newCentroids, NullWritable.get());
		}
	}

	/*
	 * Calculate distance between points as lists
	 */
	public static final double distance(String[] stringlist, double[] doublelist) {
		double sum = 0;
		for (int i = 0; i < stringlist.length; i++) {
			sum += Math
					.sqrt((Double.parseDouble(stringlist[i]) - doublelist[i])
							* (Double.parseDouble(stringlist[i]) - doublelist[i]));
		}
		return sum;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Kmeans");
		job.setJarByClass(Kmeans.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.addCacheFile(new Path("/user/jgreenberger/hw3/input/centroids.txt").toUri());
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}