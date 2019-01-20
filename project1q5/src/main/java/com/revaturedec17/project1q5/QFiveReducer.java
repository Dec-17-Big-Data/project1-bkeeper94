package com.revaturedec17.project1q5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QFiveReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String[] results = new String[4];
		for (Text value : values) {
			String[] valArr = value.toString().split(",");
			String[] codeParts = valArr[1].split("[.]");
			if (codeParts[2].compareTo("DURS") == 0) {
				if (codeParts[3].compareTo("FE") == 0) {
					results[0] = valArr[0];
				} else if (codeParts[3].compareTo("MA") == 0) {
					results[2] = valArr[0];
				}
			} else if (codeParts[2].compareTo("PROC") == 0) {
				if (codeParts[3].compareTo("FE") == 0) {
					results[1] = valArr[0];
				} else if (codeParts[3].compareTo("MA") == 0) {
					results[3] = valArr[0];
				}
			}
		}
		String outputVal = results[0] + "-days to start business (female):" + results[1]
				+ "-procedures to register business (female):" + results[2] + "-days to start business (male):"
				+ results[3] + "-procedures to register business (male)";
		context.write(new Text(key), new Text(outputVal));
	}
}