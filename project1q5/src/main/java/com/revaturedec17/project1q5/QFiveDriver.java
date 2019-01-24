package com.revaturedec17.project1q5;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class QFiveDriver {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: Project1Q5 <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(QFiveDriver.class);
		job.setJobName("Project1Q5 Driver");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(QFiveMapper.class);
		job.setReducerClass(QFiveReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
