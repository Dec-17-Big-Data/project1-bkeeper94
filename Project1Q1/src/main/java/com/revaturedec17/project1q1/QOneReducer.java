package com.revaturedec17.project1q1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QOneReducer extends Reducer<Text, Text, Text, Text> {
	/**
	 * This method takes in one of the values from the key-value pairs returned by QOneMapper.
	 * Each value in a key-value pair has the following form:
	 * "Year: (Year)--Percentage: (percentage)"
	 * This method returns the percentage as a double for use in the reduce method 
	 * @param value
	 * @return
	 */
	double getPercent(Text value) {
		String yr = "Year: ";
		String prcnt = "--Percentage: ";
		return Double.valueOf(value.toString().substring(yr.length() + 4 + prcnt.length()));
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			if (getPercent(value) < 30.0) {
				context.write(key, value);
			}
		}
	}
}