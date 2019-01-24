package com.revaturedec17.project1q4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class QFourMapper extends Mapper<LongWritable, Text, Text, Text> {
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
	 * determine which rows contain at least two data points in the interval
	 * 2000 and 2016 and which do not.
	 */
	boolean hasTwoPlusDataPoints(String[] lineArr) {
		int dataCount = 0;
		for (int i = lineArr.length - (2016 - 2000 + 1); 
				i < lineArr.length; i++) {
			if (lineArr[i].compareTo("\"\"") != 0) {
				dataCount++;
			}
			if (dataCount >=2) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * This method searches the fourth element of each line of the csv file. This
	 * fourth element of all rows except the first row is a code for the data entry
	 * being represented by a given line.
	 * This method specifically searches for codes that
	 * correspond to data entries of the national estimate of the
	 * employment to population ratio, as applied to females of 15+ years of age.
	 * 
	 * @param lineArr
	 * @return
	 **/
	boolean isValidFemaleEmploymentLine(String indicatorCode) {
		indicatorCode = indicatorCode.replaceAll("\"", "");
		return indicatorCode.compareTo("SL.EMP.TOTL.SP.FE.NE.ZS") == 0;
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
	 * isValidFemaleEducationalAttainmentLine and hasTwoPlusDataPoints.
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
	 * This method extracts the data point from the year closest to
	 * or equal to the year 2000.
	 * This method only searches those lines of the csv file that 
	 * satisfy the methods isValidFemaleEducationalAttainmentLine 
	 * and hasTwoPlusDataPoints.
	 * 
	 * @param lineArr
	 * @return
	 **/
	String[] getLeastCurrentDataPoint(String[] validLineArr, int[] years) {
		String[] result = new String[2];
		Double currValue = 0.0;
		int currValueInd = 0;
		for (int i = validLineArr.length - 1; 
				i >= validLineArr.length - (2016 - 2000 + 1); i--) {
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
	 * This method receives the output from getMostCurrentDataPoint and the
	 * output from getLeastCurrentDataPoint and combines them into
	 * a single string
	 * 
	 * @param validLineArr
	 * @return
	 */
	private String buildValue(String[] validLineArr) {
		String[] result1 = getLeastCurrentDataPoint(validLineArr, getYears());
		String[] result2 = getMostCurrentDataPoint(validLineArr, getYears());
		return result1[0] + "," + result2[0] + ":" + result1[1] + "," + result2[1];
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineArr = prepareLine(removeExtraCommas(line));
		if (isValidFemaleEmploymentLine(lineArr[3]) && hasTwoPlusDataPoints(lineArr)) {
			// The country name is the key in each key-value pair
			context.write(new Text(lineArr[0].replaceAll("\"", "")), new Text(buildValue(lineArr)));
		}
	}
}
