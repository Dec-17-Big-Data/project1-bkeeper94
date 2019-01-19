package com.revaturedec17.project1q5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QFiveReducer extends Reducer<Text, Text, Text, Text> {
	/*
	 * This method finds those countries where it took less than 45 days to start up a business and had
	 * ten or fewer procedures to register a business
	*/
	
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			
			context.write(new Text(key), new Text(value));
		}
	}
}