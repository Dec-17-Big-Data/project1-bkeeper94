package com.revaturedec17.project1q3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QThreeReducer extends Reducer<Text, Text, Text, Text> {
	/**
	 * This method finds the most current data entry and the least current
	 * data entry within the interval of values specified by 'input.'
	 * After the left-most and right-most values are obtained,
	 * the percent change between these two values is calculated
	 * and returned.
	 * 
	 * @param input
	 * @return
	 */
	String getOverallPrcntChange(String input) {
		String [] percents = input.split("-");
		Double currPrcnt = 0.0;
		Double pastPrcnt = 0.0;
		double scale = Math.pow(10, 2); // used to round each difference
		for (int i = 0; i < percents.length; i++) {
			if (percents[percents.length- 1- i].compareTo("\"\"") == 0) {
				continue;
			} else {
				pastPrcnt = Double.valueOf(percents[percents.length - 1 - i]);
			}
			if (percents[i].compareTo("\"\"") == 0) {
				continue;
			} else {
				currPrcnt = Double.valueOf(percents[i]);
			}
			
		}
		// round the result to three decimal places
		Double result = 100 * (currPrcnt - pastPrcnt ) / pastPrcnt;
		result = (Math.round(result * scale) / scale);
		return String.format ("%.2f", result); // makes sure the result outputs correctly
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			String input = value.toString();
			String output = getOverallPrcntChange(input);
			context.write(new Text(key), new Text(output));
		}
	}
}