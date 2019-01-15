package com.revaturedec17.project1q3;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class QThreeMapper extends Mapper<LongWritable, Text, Text, Text> {
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
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineArr = prepareLine(removeExtraCommas(line));
		/*
		if (isUnitedStatesData(lineArr[1]) && isValidFemaleEnrollmentLine(lineArr[3])) {
			String[] valArr = getDataFrom2000Onward(lineArr);
			context.write(new Text(lineArr[2].replaceAll("\"", "")), new Text(buildValue(valArr)));
		}
		*/
	}
}
