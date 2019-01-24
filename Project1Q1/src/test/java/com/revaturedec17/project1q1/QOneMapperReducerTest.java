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
	 *  These 3 MRUnit methods will test map-reduce on one of the rows that will
	 *  contribute to the overall output of the hadoop job.
	 */
	
	@Test
	public void testMapper() {
		String record1 = "\"China\",\"CHN\",\"Gross graduation ratio, tertiary, female (%)\","
				+ "\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.57285\",\"24.49351\","
				+ "\"28.21447\",\"\",";
		String expectedKey1 = "China";
		String expectedValue1 = "Year: 2015--Percentage: 28.21447";
		mapDriver.withInput(new LongWritable(), new Text(record1));
		mapDriver.withOutput(new Text(expectedKey1), new Text(expectedValue1));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		String inputVal1 = "Year: 2015--Percentage: 28.21447";
		List<Text> values1 = new ArrayList<Text>();
		values1.add(new Text(inputVal1));
		String key1 = "Country: United States";
		String expectedValue1 = "Year: 2015--Percentage: 28.21447";
		reduceDriver.withInput(new Text(key1), values1);
		reduceDriver.withOutput(new Text(key1), new Text(expectedValue1));
		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		String record1 = "\"China\",\"CHN\",\"Gross graduation ratio, tertiary, female (%)\","
				+ "\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.57285\",\"24.49351\","
				+ "\"28.21447\",\"\",";
		String expectedKeyResult1 = "China";
		String expectedValueResult1 = "Year: 2015--Percentage: 28.21447";
		mapReduceDriver.withInput(new LongWritable(), new Text(record1));
		mapReduceDriver.withOutput(new Text(expectedKeyResult1), new Text(expectedValueResult1));
		mapReduceDriver.runTest();
	}
	
}
