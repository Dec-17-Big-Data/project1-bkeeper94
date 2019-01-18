package com.revaturedec17.project1q5;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class QFiveMapper extends Mapper<LongWritable, Text, Text, Text> {
	/**
	 * This method prepares a line of the csv file by eliminating any comma that is
	 * not a comma delimiter.
	 * 
	 * @param badLine
	 * @return
	 */
	String removeExtraCommas(String badLine) {
		return badLine.replaceAll(", ", " - ");
	}

	/**
	 * This method transforms a line of the csv file into an array by splitting it
	 * at every comma. 
	 * This method only takes in a line that has already been passed
	 * through the method removeExtraCommas.
	 * 
	 * @param goodLine
	 * @return
	 */
	String[] prepareLine(String goodLine) {
		return goodLine.split(",");
	}

	/**
	 * This method searches the data points within each row of the csv file to
	 * determine which rows contain data and which rows do not.
	 */
	boolean hasDataPoints(String[] lineArr) {
		for (int i = 4; i < lineArr.length; i++) {
			if (lineArr[i].compareTo("\"\"") != 0) {
				return true;
			}
		}
		return false;
	}

	/**
	 * This method searches the fourth element of each line of the csv file. This
	 * fourth element of all rows except the first row is a code for the data entry
	 * being represented by a given line. This method specifically searches for
	 * codes corresponding to data entries that provide business start-up
	 * information. The two pieces of data from this set that are of interest are
	 * the number of procedures needed to register a business and the time in days
	 * required to start a business.
	 * 
	 * @param lineArr
	 * @return
	 **/
	boolean isValidStartupLine(String indicatorCode) {
		indicatorCode = indicatorCode.replaceAll("\"", "");
		return indicatorCode.compareTo("IC.REG.PROC.FE") == 0 || indicatorCode.compareTo("IC.REG.DURS.FE") == 0
				|| indicatorCode.compareTo("IC.REG.PROC.MA") == 0 || indicatorCode.compareTo("IC.REG.DURS.MA") == 0;
	}

	/**
	 * This method searches the data that passes through the methods hasDataPoints
	 * and isValidStartupLine and returns each entry from the year 2003. Each 2003
	 * data point serves as the value in all key-values pairs generated from the map
	 * method.
	 * 
	 * @param lineArr
	 * @return
	 */
	String get2003DataPoint(String[] lineArr) {
		return lineArr[lineArr.length - (2016 - 2003 + 1)];
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineArr = prepareLine(removeExtraCommas(line));
		if (isValidStartupLine(lineArr[3]) && hasDataPoints(lineArr)) {
			// The country name is the key in each key-value pair
			context.write(new Text(lineArr[0].replaceAll("\"", "")), new Text(get2003DataPoint(lineArr)));
		}
	}
}
