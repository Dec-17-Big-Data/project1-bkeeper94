package com.revaturedec17.project1q5;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class QFiveMapperReducerTest {
	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
	
	@Before
	public void setUp() {
		QFiveMapper mapper = new QFiveMapper();
		QFiveReducer reducer = new QFiveReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}
	
	/**
	 * These 3 MRUnit test methods will test map-reduce on one of the rows of the csv file that 
	 * will contribute to the overall output of the hadoop job.
	 */
	
	@Test
	public void testMapper() {
		String record1 = "\"China\",\"CHN\",\"Time required to start up a business, female (days)\",\"IC.REG.DURS.FE\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"48\",\"48\",\"48\",\"35\",\"35\",\"41\",\"38\",\"38\",\"38\",\"33\",\"34.4\",\"31.4\",\"31.4\",\"28.9\",";
		String expectedKey1 = "China";
		String expectedValue1 = "48,IC.REG.DURS.FE";
		mapDriver.withInput(new LongWritable(), new Text(record1));
		mapDriver.withOutput(new Text(expectedKey1), new Text(expectedValue1));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		String inputVal1 = "6,IC.REG.PROC.FE";
		String inputVal2 = "6,IC.REG.PROC.MA";
		String inputVal3 = "6,IC.REG.DURS.FE";
		String inputVal4 = "6,IC.REG.DURS.MA";
		List<Text> values1 = new ArrayList<Text>();
		values1.add(new Text(inputVal1));
		values1.add(new Text(inputVal2));
		values1.add(new Text(inputVal3));
		values1.add(new Text(inputVal4));
		String key1 = "United States";
		String expectedValue1 = "6,days to start business,6,procedures to register business";
		reduceDriver.withInput(new Text(key1), values1);
		reduceDriver.withOutput(new Text(key1), new Text(expectedValue1));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		String record1 = "\"United States\",\"USA\",\"Time required to start up a business, female (days)\","
				+ "\"IC.REG.DURS.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"6\",\"6\",\"6\",\"6\",\"6\",\"5\","
				+ "\"5\",\"5\",\"5\",\"5\",\"6.2\",\"5.6\",\"5.6\",\"5.6\",";
		String record2 = "\"United States\",\"USA\",\"Start-up procedures to register a business, female (number)\","
				+ "\"IC.REG.PROC.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\","
				+ "\"6\",\"6\",\"6\",";
		String record3 = "\"United States\",\"USA\",\"Start-up procedures to register a business, male (number)\","
				+ "\"IC.REG.PROC.MA\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\","
				+ "\"6\",\"6\",\"6\",\"6\",\"6\",";
		String record4 = "\"United States\",\"USA\",\"Time required to start up a business, male (days)\","
				+ "\"IC.REG.DURS.MA\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"6\",\"6\",\"6\",\"6\",\"6\",\"5\","
				+ "\"5\",\"5\",\"5\",\"5\",\"6.2\",\"5.6\",\"5.6\",\"5.6\",";
		String expectedKeyResult1 = "United States";
		String expectedValueResult1 = "6,days to start business,6,procedures to register business";
		mapReduceDriver.withInput(new LongWritable(), new Text(record1));
		mapReduceDriver.withInput(new LongWritable(), new Text(record2));
		mapReduceDriver.withInput(new LongWritable(), new Text(record3));
		mapReduceDriver.withInput(new LongWritable(), new Text(record4));
		mapReduceDriver.withOutput(new Text(expectedKeyResult1), new Text(expectedValueResult1));
		mapReduceDriver.runTest();
	}
}
