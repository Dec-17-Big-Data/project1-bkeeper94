package com.revaturedec17.project1q5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QFiveReducer extends Reducer<Text, Text, Text, Text> {
	/**
	 * The reduce method takes all four key-value pairs for each country or
	 * region and places them into a string that will become the output
	 * value associated with key (the country or region).
	 * The 2003 value for time required to start a business must be less 
	 * than 45 days and the 2003 value for the number of procedures to 
	 * register a business must be less than 10.
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String[] results = new String[4];
		for (Text value : values) {
			String[] valArr = value.toString().split(",");
			//make sure all values are turned into whole numbers
			Double floorVal = Math.floor(Double.valueOf(valArr[0]));
			Integer roundedVal = floorVal.intValue();
			valArr[0] = roundedVal.toString();
			//identify the current member of the values Iterable
			String[] codeParts = valArr[1].split("[.]");
			if (codeParts[2].compareTo("DURS") == 0 && Integer.valueOf(valArr[0]) < 45) {
				if (codeParts[3].compareTo("FE") == 0) {
					results[0] = valArr[0];
				} else if (codeParts[3].compareTo("MA") == 0) {
					results[2] = valArr[0];
				}
			} else if (codeParts[2].compareTo("PROC") == 0 && Integer.valueOf(valArr[0]) <= 10) {
				if (codeParts[3].compareTo("FE") == 0) {
					results[1] = valArr[0];
				} else if (codeParts[3].compareTo("MA") == 0) {
					results[3] = valArr[0];
				}
			} else {
				//makes sure that records that fail to satisfy either 
				//condition are not included in final result
				return;
			}
		}
		String outputVal = results[0] + "-days to start business (female):" + results[1]
				+ "-procedures to register business (female):" + results[2] + "-days to start business (male):"
				+ results[3] + "-procedures to register business (male)";
		context.write(new Text(key), new Text(outputVal));
	}
}