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
	 * These 3 MRUnit test methods will test map-reduce on a few of the rows in the
	 * csv file that may contribute to the overall output of this hadoop job.
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
	public void testMapperAgain() {
		String record1 = "\"Europe & Central Asia\",\"ECS\",\"Start-up procedures to register a business, female (number)\",\"IC.REG.PROC.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"9.47619047619048\",\"8.88372093023256\",\"8.88636363636364\",\"8.45652173913044\",\"8.10869565217391\",\"7.27659574468085\",\"6.79166666666667\",\"6.54166666666667\",\"6.22916666666667\",\"6.14285714285714\",\"5.63469387755102\",\"5.37551020408163\",\"5.08979591836735\",\"4.9734693877551\",";
		String expectedKey1 = "Europe & Central Asia";
		String expectedValue1 = "9.47619047619048,IC.REG.PROC.FE";
		mapDriver.withInput(new LongWritable(), new Text(record1));
		mapDriver.withOutput(new Text(expectedKey1), new Text(expectedValue1));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		String inputVal11 = "6,IC.REG.PROC.FE";
		String inputVal12 = "6,IC.REG.PROC.MA";
		String inputVal13 = "6,IC.REG.DURS.FE";
		String inputVal14 = "6,IC.REG.DURS.MA";
		List<Text> values1 = new ArrayList<Text>();
		values1.add(new Text(inputVal11));
		values1.add(new Text(inputVal12));
		values1.add(new Text(inputVal13));
		values1.add(new Text(inputVal14));
		String key1 = "United States";
		String expectedValue1 = "6-days to start business (female):6-procedures to register business (female):"
				+ "6-days to start business (male):6-procedures to register business (male)";
		reduceDriver.withInput(new Text(key1), values1);
		reduceDriver.withOutput(new Text(key1), new Text(expectedValue1));
		reduceDriver.runTest();
	}

	@Test
	public void testReducerAgain() {
		String inputVal21 = "9.47619047619048,IC.REG.PROC.FE";
		String inputVal22 = "9.47619047619048,IC.REG.PROC.MA";
		String inputVal23 = "42.7738095238095,IC.REG.DURS.FE";
		String inputVal24 = "42.7738095238095,IC.REG.DURS.MA";
		List<Text> values2 = new ArrayList<Text>();
		values2.add(new Text(inputVal21));
		values2.add(new Text(inputVal22));
		values2.add(new Text(inputVal23));
		values2.add(new Text(inputVal24));
		String key2 = "Europe and Central Asia";
		String expectedValue1 = "42-days to start business (female):9-procedures to register business (female):"
				+ "42-days to start business (male):9-procedures to register business (male)";
		reduceDriver.withInput(new Text(key2), values2);
		reduceDriver.withOutput(new Text(key2), new Text(expectedValue1));
		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		String record11 = "\"United States\",\"USA\",\"Time required to start up a business, female (days)\","
				+ "\"IC.REG.DURS.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"6\",\"6\",\"6\",\"6\",\"6\",\"5\","
				+ "\"5\",\"5\",\"5\",\"5\",\"6.2\",\"5.6\",\"5.6\",\"5.6\",";
		String record12 = "\"United States\",\"USA\",\"Start-up procedures to register a business, female (number)\","
				+ "\"IC.REG.PROC.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\","
				+ "\"6\",\"6\",\"6\",";
		String record13 = "\"United States\",\"USA\",\"Start-up procedures to register a business, male (number)\","
				+ "\"IC.REG.PROC.MA\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\",\"6\","
				+ "\"6\",\"6\",\"6\",\"6\",\"6\",";
		String record14 = "\"United States\",\"USA\",\"Time required to start up a business, male (days)\","
				+ "\"IC.REG.DURS.MA\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"6\",\"6\",\"6\",\"6\",\"6\",\"5\","
				+ "\"5\",\"5\",\"5\",\"5\",\"6.2\",\"5.6\",\"5.6\",\"5.6\",";
		String expectedKeyResult1 = "United States";
		String expectedValueResult1 = "6-days to start business (female):6-procedures to register business (female):"
				+ "6-days to start business (male):6-procedures to register business (male)";
		mapReduceDriver.withInput(new LongWritable(), new Text(record11));
		mapReduceDriver.withInput(new LongWritable(), new Text(record12));
		mapReduceDriver.withInput(new LongWritable(), new Text(record13));
		mapReduceDriver.withInput(new LongWritable(), new Text(record14));
		mapReduceDriver.withOutput(new Text(expectedKeyResult1), new Text(expectedValueResult1));
		mapReduceDriver.runTest();
	}

	@Test
	public void testMapReduceAgain() {
		String record21 = "\"Europe & Central Asia\",\"ECS\",\"Start-up procedures to register a business, female (number)\","
				+ "\"IC.REG.PROC.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"9.47619047619048\",\"8.88372093023256\",\"8.88636363636364\",\"8.45652173913044\","
				+ "\"8.10869565217391\",\"7.27659574468085\",\"6.79166666666667\",\"6.54166666666667\",\"6.22916666666667\","
				+ "\"6.14285714285714\",\"5.63469387755102\",\"5.37551020408163\",\"5.08979591836735\",\"4.9734693877551\",";
		String record22 = "\"Europe & Central Asia\",\"ECS\",\"Start-up procedures to register a business, male (number)\","
				+ "\"IC.REG.PROC.MA\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"9.47619047619048\",\"8.88372093023256\",\"8.88636363636364\",\"8.45652173913044\","
				+ "\"8.10869565217391\",\"7.27659574468085\",\"6.79166666666667\",\"6.54166666666667\",\"6.22916666666667\","
				+ "\"6.14285714285714\",\"5.63469387755102\",\"5.37551020408163\",\"5.08979591836735\",\"4.9734693877551\",";
		String record23 = "\"Europe & Central Asia\",\"ECS\",\"Time required to start up a business, female (days)\","
				+ "\"IC.REG.DURS.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"42.7738095238095\",\"37.5\",\"32.6477272727273\",\"27.8586956521739\","
				+ "\"24.054347826087\",\"20.2446808510638\",\"17.96875\",\"16.6145833333333\",\"15.1875\",\"14.8673469387755\","
				+ "\"13.6918367346939\",\"12.2387755102041\",\"10.1632653061224\",\"9.73061224489796\",";
		String record24 = "\"Europe & Central Asia\",\"ECS\",\"Time required to start up a business, male (days)\","
				+ "\"IC.REG.DURS.MA\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"42.7738095238095\",\"37.5\",\"32.6477272727273\",\"27.8586956521739\","
				+ "\"24.054347826087\",\"20.2446808510638\",\"17.96875\",\"16.6145833333333\",\"15.1875\",\"14.8673469387755\","
				+ "\"13.6918367346939\",\"12.2387755102041\",\"10.1632653061224\",\"9.73061224489796\",";
		String expectedKeyResult2 = "Europe & Central Asia";
		String expectedValueResult2 = "42-days to start business (female):9-procedures to register business (female):"
				+ "42-days to start business (male):9-procedures to register business (male)";
		mapReduceDriver.withInput(new LongWritable(), new Text(record21));
		mapReduceDriver.withInput(new LongWritable(), new Text(record22));
		mapReduceDriver.withInput(new LongWritable(), new Text(record23));
		mapReduceDriver.withInput(new LongWritable(), new Text(record24));
		mapReduceDriver.withOutput(new Text(expectedKeyResult2), new Text(expectedValueResult2));
		mapReduceDriver.runTest();
	}
}
