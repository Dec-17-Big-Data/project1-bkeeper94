package com.revaturedec17.project1q1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class QOneMapper extends Mapper<LongWritable, Text, Text, Text> {
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
	 * at every comma. This method only takes in a line that has already been passed
	 * through the methods removeExtraCommas, removeQuotes, and
	 * fillInEmptyDataEntries. This method checks if the array has one extra element
	 * at the end and "removes" it by creating a copy of the new array that does not
	 * have that last element
	 * 
	 * @param goodLine
	 * @return
	 */
	String[] prepareLine(String goodLine) {
		return goodLine.split(",");
	}

	/**
	 * This method searches the data points within each row of the csv file to
	 * determine which rows contain data and which rows do not
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
	 * being represented by a given line. This method searches for codes
	 * corresponding to data entries pertaining to females who have completed any
	 * one of the levels of tertiary education (bachelor's, master's, doctorate).
	 * 
	 * @param lineArr
	 * @return
	 **/
	boolean isValidFemaleGradLine(String indicatorCode) {
		indicatorCode = indicatorCode.replaceAll("\"", "");
		return indicatorCode.compareTo("SE.TER.HIAT.BA.FE.ZS") == 0
				|| indicatorCode.compareTo("SE.TER.HIAT.DO.FE.ZS") == 0
				|| indicatorCode.compareTo("SE.TER.HIAT.MS.FE.ZS") == 0;
	}
	
	/**
	 * This method returns the country name and indicator name of those rows of the
	 * csv file that satisfy the conditions provided by
	 * isValidFemaleEducationalAttainment and hasDataPoints
	 * 
	 * @param validLineArr
	 * @return
	 */
	private String buildKey(String[] validLineArr) {
		return "Country: " + validLineArr[0].replaceAll("\"", "") + "--" + "Data Name: "
				+ validLineArr[2].replaceAll("\"", "");
	}

	/**
	 * Helper method that returns an int array containing the years from 1960 to
	 * 2016
	 * 
	 * @return
	 */
	int[] getYears() {
		int yearOne = 1960;
		int yearEnd = 2016;
		int[] years = new int[yearEnd - yearOne + 1];
		years[0] = yearOne;
		for (int i = 1; i < years.length; i++) {
			years[i] = years[i - 1] + 1;
		}
		return years;
	}

	/**
	 * This method extracts the most current data point and the corresponding year
	 * from those lines of the csv file that satisfy the methods
	 * isValidFemaleEducationalAttainmentLine and hasDataPoints.
	 * 
	 * @param lineArr
	 * @return
	 **/
	String[] getMostCurrentDataPoint(String[] validLineArr, int[] years) {
		String[] result = new String[2];
		Double currValue = 0.0;
		int currValueInd = 0;
		for (int i = 4; i < validLineArr.length; i++) {
			if (validLineArr[i].compareTo("\"\"") == 0) {
				continue;
			}
			validLineArr[i] = validLineArr[i].replaceAll("\"", "");
			currValue = Double.valueOf(validLineArr[i]);
			currValueInd = i;
		}
		result[0] = ((Integer) years[currValueInd - 4]).toString();
		result[1] = currValue.toString();
		return result;
	}

	/**
	 * This method combines the values from the method getMostCurrentDataPoint into
	 * a single string
	 * 
	 * @param validLineArr
	 * @return
	 */
	private String buildValue(String[] validLineArr) {
		String[] result = getMostCurrentDataPoint(validLineArr, getYears());
		return "Year: " + result[0] + "--" + "Percentage: " + result[1];
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineArr = prepareLine(removeExtraCommas(line));
		if (isValidFemaleGradLine(lineArr[3]) && hasDataPoints(lineArr)) {
			context.write(new Text(buildKey(lineArr)), new Text(buildValue(lineArr)));
		}
	}
}
