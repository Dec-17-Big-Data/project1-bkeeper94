package com.revaturedec17.project1q1;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class QOneMapperReducerTest {
	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		QOneMapper mapper = new QOneMapper();
		QOneReducer reducer = new QOneReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}
	
	/**
	 *  These 9 MRUnit methods will test map-reduce on 3 of the rows that will
	 *  contribute to the overall output of the hadoop job.
	 */
	
	@Test
	public void testMapper() {
		String record1 = "\"Bolivia\",\"BOL\",\"Educational attainment, completed Master's or equivalent, "
				+ "population 25+ years, female (%)\",\"SE.TER.HIAT.MS.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"1.21635\",\"\",\"\",\"\",\"\",";
		String expectedKey1 = "Country: Bolivia--Data Name: Educational attainment - completed Master's or equivalent - "
				+ "population 25+ years - female (%)";
		String expectedValue1 = "Year: 2012--Percentage: 1.21635";
		mapDriver.withInput(new LongWritable(), new Text(record1));
		mapDriver.withOutput(new Text(expectedKey1), new Text(expectedValue1));
		mapDriver.runTest();
	}

	@Test
	public void testMapperAgain() {
		String record2 = "\"Bolivia\",\"BOL\",\"Educational attainment, completed Bachelor's or equivalent, "
				+ "population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"3.3\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"7.9\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.85593\","
				+ "\"\",\"\",\"\",\"\",\"19.20283\",\"\",\"18.65752\",\"20.97529\",\"\",\"\",\"20.6348\",\"\",\"\",\"\",\"\",";
		String expectedKey2 = "Country: Bolivia--Data Name: Educational attainment - completed Bachelor's or equivalent - "
				+ "population 25+ years - female (%)";
		String expectedValue2 = "Year: 2012--Percentage: 20.6348";
		mapDriver.withInput(new LongWritable(), new Text(record2));
		mapDriver.withOutput(new Text(expectedKey2), new Text(expectedValue2));
		mapDriver.runTest();
	}

	@Test
	public void testMapperOneMoreTime() {
		String record3 = "\"Bolivia\",\"BOL\",\"Educational attainment, completed Doctoral or equivalent, "
				+ "population 25+ years, female (%)\",\"SE.TER.HIAT.DO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.20538\",\"\",\"\",\"\",\"\",";
		String expectedKey3 = "Country: Bolivia--Data Name: Educational attainment - completed Doctoral or equivalent - "
				+ "population 25+ years - female (%)";
		String expectedValue3 = "Year: 2012--Percentage: 0.20538";
		mapDriver.withInput(new LongWritable(), new Text(record3));
		mapDriver.withOutput(new Text(expectedKey3), new Text(expectedValue3));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		String inputVal1 = "Year: 2012--Percentage: 1.21635";
		List<Text> values1 = new ArrayList<Text>();
		values1.add(new Text(inputVal1));
		String key1 = "Country: Bolivia--Data Name: Educational attainment - completed Master's or equivalent - "
				+ "population 25+ years - female (%)";
		String expectedValue1 = "Year: 2012--Percentage: 1.21635";
		reduceDriver.withInput(new Text(key1), values1);
		reduceDriver.withOutput(new Text(key1), new Text(expectedValue1));
		reduceDriver.runTest();
	}

	@Test
	public void testReducerAgain() {
		String inputVal2 = "Year: 2012--Percentage: 20.6348";
		List<Text> values2 = new ArrayList<Text>();
		values2.add(new Text(inputVal2));
		String key2 = "Country: Bolivia--Data Name: Educational attainment - completed Bachelor's or equivalent - "
				+ "population 25+ years - female (%)";
		String expectedValue2 = "Year: 2012--Percentage: 20.6348";
		reduceDriver.withInput(new Text(key2), values2);
		reduceDriver.withOutput(new Text(key2), new Text(expectedValue2));
		reduceDriver.runTest();
	}

	@Test
	public void testReducerOneMoreTime() {
		String inputVal3 = "Year: 2012--Percentage: 0.20538";
		List<Text> values3 = new ArrayList<Text>();
		values3.add(new Text(inputVal3));
		String key3 = "Country: Bolivia--Data Name: Educational attainment - completed Doctoral or equivalent - "
				+ "population 25+ years - female (%)";
		String expectedValue3 = "Year: 2012--Percentage: 0.20538";
		reduceDriver.withInput(new Text(key3), values3);
		reduceDriver.withOutput(new Text(key3), new Text(expectedValue3));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		String record1 = "\"Bolivia\",\"BOL\",\"Educational attainment, completed Master's or equivalent, "
				+ "population 25+ years, female (%)\",\"SE.TER.HIAT.MS.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"1.21635\",\"\",\"\",\"\",\"\",";
		String expectedKeyResult1 = "Country: Bolivia--Data Name: Educational attainment - completed Master's or equivalent - "
				+ "population 25+ years - female (%)";
		String expectedValueResult1 = "Year: 2012--Percentage: 1.21635";
		mapReduceDriver.withInput(new LongWritable(), new Text(record1));
		mapReduceDriver.withOutput(new Text(expectedKeyResult1), new Text(expectedValueResult1));
		mapReduceDriver.runTest();
	}
	
	@Test
	public void testMapReduceAgain() {
		String record2 = "\"Bolivia\",\"BOL\",\"Educational attainment, completed Bachelor's or equivalent, "
				+ "population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"3.3\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"7.9\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.85593\","
				+ "\"\",\"\",\"\",\"\",\"19.20283\",\"\",\"18.65752\",\"20.97529\",\"\",\"\",\"20.6348\",\"\",\"\",\"\",\"\",";
		String expectedKeyResult2 = "Country: Bolivia--Data Name: Educational attainment - completed Bachelor's or equivalent - " + 
				"population 25+ years - female (%)";
		String expectedValueResult2 = "Year: 2012--Percentage: 20.6348";
		mapReduceDriver.withInput(new LongWritable(), new Text(record2));
		mapReduceDriver.withOutput(new Text(expectedKeyResult2), new Text(expectedValueResult2));
		mapReduceDriver.runTest();
	}
	
	@Test
	public void testMapReduceOneMoreTime() {
		String record3 = "\"Bolivia\",\"BOL\",\"Educational attainment, completed Doctoral or equivalent, "
				+ "population 25+ years, female (%)\",\"SE.TER.HIAT.DO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.20538\",\"\",\"\",\"\",\"\",";
		String expectedKeyResult3 = "Country: Bolivia--Data Name: Educational attainment - completed Doctoral or equivalent - "
				+ "population 25+ years - female (%)";
		String expectedValueResult3 = "Year: 2012--Percentage: 0.20538";
		mapReduceDriver.withInput(new LongWritable(), new Text(record3));
		mapReduceDriver.withOutput(new Text(expectedKeyResult3), new Text(expectedValueResult3));
		mapReduceDriver.runTest();
	}
}
