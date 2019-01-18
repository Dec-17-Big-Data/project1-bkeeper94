package com.revaturedec17.project1q4;

import java.io.IOException;
import java.util.Arrays;

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
	 * This method takes in data row arrays that pass through the methods hasDataPoints
	 * and isValidFemaleEmploymentLine and returns those entries
	 * that are from the year 2000 onward.
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
	 * the array; all elements are separated by dashes.
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
		if (isValidFemaleEmploymentLine(lineArr[3]) && hasDataPoints(lineArr)) {
			String[] valArr = getDataFrom2000Onward(lineArr);
			// The country name is the key in each key-value pair
			context.write(new Text(lineArr[0].replaceAll("\"", "")), new Text(buildValue(valArr)));
		}
	}
}
