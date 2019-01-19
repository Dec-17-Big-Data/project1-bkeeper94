package com.revaturedec17.project1q3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QThreeReducer extends Reducer<Text, Text, Text, Text> {
	/**
	 * This method takes in the most current data entry and the least current
	 * data entry within the interval of 2000 to 2016.
	 * The percent change between these two values is calculated
	 * and returned.
	 * 
	 * @param input
	 * @return
	 */
	String getOverallPrcntChange(String input) {
		String [] percents = input.split(",");
		Double pastPrcnt = Double.valueOf(percents[0]);
		Double currPrcnt = Double.valueOf(percents[1]);
		// round the result to three decimal places
		double scale = Math.pow(10, 2);
		Double result = 100 * (currPrcnt - pastPrcnt ) / pastPrcnt;
		result = (Math.round(result * scale) / scale);
		// makes sure the result outputs correctly
		return String.format ("%.2f", result);
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			String[] input = value.toString().split(":");
			String output = getOverallPrcntChange(input[1]);
			context.write(new Text(key), new Text(input[0] + ":" + output));
		}
	}
}