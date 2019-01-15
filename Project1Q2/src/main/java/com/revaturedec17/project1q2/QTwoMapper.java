package com.revaturedec17.project1q2;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class QTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
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
	 * have that last element.
	 * 
	 * @param goodLine
	 * @return
	 */
	String[] prepareLine(String goodLine) {
		return goodLine.split(",");
	}

	// Test with JUnit
	/**
	 * This method checks the second element of each line of the csv file and
	 * determines if the country code is USA
	 * 
	 * @param countryName
	 * @return
	 */
	boolean isUnitedStatesData(String countryCode) {
		return countryCode.contains("USA");
	}

	/**
	 * This method searches the fourth element of each line of the csv file. This
	 * fourth element of all rows except the first row is a code for the data entry
	 * being represented by a given line. This method searches for codes
	 * corresponding to the Gender Parity Index (GPI) measured for the U.S. primary,
	 * secondary, and tertiary education systems. This method will also extract the
	 * GPI measured across the U.S.'s primary and secondary education programs.
	 * 
	 * @param indicatorCode
	 * @return
	 */
	boolean isValidFemaleEnrollmentLine(String indicatorCode) {
		indicatorCode = indicatorCode.replaceAll("\"", "");
		return indicatorCode.compareTo("SE.ENR.PRIM.FM.ZS") == 0 || indicatorCode.compareTo("SE.ENR.PRSC.FM.ZS") == 0
				|| indicatorCode.compareTo("SE.ENR.SECO.FM.ZS") == 0
				|| indicatorCode.compareTo("SE.ENR.TERT.FM.ZS") == 0;
	}

	/**
	 * This method searches the data that passes through the methods
	 * isUnitedStatesData and isValidFemaleEnrollmentLine and returns those entries
	 * that are from the year 2000 onward
	 * 
	 * @param lineArr
	 * @return
	 */
	String[] getDataFrom2000Onward(String[] lineArr) {
		return Arrays.copyOfRange(lineArr, lineArr.length - (2016 - 2000 + 1), lineArr.length);
	}

	/**
	 * This method transforms the array returned by getDataFrom2000Onward into a
	 * string containing the data entries from the array and any empty entries from
	 * the array; all elements are separated by dashes
	 * 
	 * @param valArr
	 * @return
	 */
	String buildValue(String[] valArr) {
		StringBuffer value = new StringBuffer();
		for (int i = 0; i < valArr.length; i++) {
			if (valArr[i].compareTo("\"\"") != 0) {
				valArr[i] = valArr[i].replaceAll("\"", "");
			}
			if (i == valArr.length - 1) {
				value.append(valArr[i]);
			} else {
				value.append(valArr[i] + "-");
			}
		}
		return value.toString();
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineArr = prepareLine(removeExtraCommas(line));
		if (isUnitedStatesData(lineArr[1]) && isValidFemaleEnrollmentLine(lineArr[3])) {
			String[] valArr = getDataFrom2000Onward(lineArr);
			// The indicator name is the key in each key-value pair
			context.write(new Text(lineArr[2].replaceAll("\"", "")), new Text(buildValue(valArr)));
		}
	}
}
