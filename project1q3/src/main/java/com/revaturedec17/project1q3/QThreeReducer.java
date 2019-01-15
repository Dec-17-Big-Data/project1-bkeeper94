package com.revaturedec17.project1q3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QThreeReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/*
		for (Text value : values) {
			String input = value.toString();
			String output = getRelativeDifferences(input);
			context.write(new Text(key), new Text(output));
		}
		*/
	}
}