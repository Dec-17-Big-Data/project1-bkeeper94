package com.revaturedec17.project1q2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QTwoReducer extends Reducer<Text, Text, Text, Text> {
	/**
	 * This method returns the percent relative difference in Gender Parity Index 
	 * between each consecutive year's value and returns the result as a string
	 * containing the results separated by commas.
	 * 
	 * Percent relative difference between a and b = 100* (b - a) / ((b+a)/2)
	 * 
	 * The relative difference is used instead of the actual difference because
	 * the GPIs are very close in value to each other. Normalization of
	 * the actual differences aids in visualizing the GPI variation.
	 * 
	 * This method adds the total number of GPI values found to the end
	 * of the output string so that the average relative difference can be easily
	 * calculated outside of hadoop.
	 * 
	 * @param input
	 * @return
	 */
	String getRelativeDifferences(String input) {
		String [] percents = input.split("-");
		StringBuilder sb = new StringBuilder();
		Double currPrcntDiff = 0.0;
		double scale = Math.pow(10, 3); // used to round each difference
		int shift = 1; // used to account for an empty entry in the middle of the input string
		int count = 0; // used to track how many differences were calculated over the interval
		for (int i = 1; i < percents.length; i++) {
			if (percents[i].compareTo("\"\"") == 0) {
				shift++;
				continue;
			}
			
			currPrcntDiff = 100 * (2 * (Double.valueOf(percents[i]) - Double.valueOf(percents[i - shift])) / 
					(Double.valueOf(percents[i]) + Double.valueOf(percents[i - shift])));
			// round the difference to three decimal places
			currPrcntDiff = (Math.round(currPrcntDiff * scale) / scale);
			sb.append(String.format ("%.3f", currPrcntDiff) + ","); // makes sure the difference outputs correctly
			shift = 1;
			count++;
		}
		sb.append("Total: "+ count);
		return sb.toString();
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value: values) {
			String input = value.toString();
			String output = getRelativeDifferences(input);
			context.write(new Text(key), new Text(output));
		}
	}
}