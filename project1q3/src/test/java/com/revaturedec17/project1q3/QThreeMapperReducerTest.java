package com.revaturedec17.project1q3;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class QThreeMapperReducerTest {
	// Change these as necessary
	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		QThreeMapper mapper = new QThreeMapper();
		QThreeReducer reducer = new QThreeReducer();
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
		String record1 = "\"Germany\",\"DEU\",\"Employment to population ratio, 15+, male (%) (national estimate)\","
				+ "\"SL.EMP.TOTL.SP.MA.NE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"66.2799987792969\",\"66.9300003051758\","
				+ "\"66.6399993896484\",\"67.0400009155273\",\"66.5500030517578\",\"67.3099975585938\","
				+ "\"67.9199981689453\",\"69.0599975585938\",\"68.6900024414063\",\"67.1399993896484\","
				+ "\"65.4199981689453\",\"64.1699981689453\",\"63.7400016784668\",\"62.5400009155273\","
				+ "\"61.5400009155273\",\"61.3400001525879\",\"61.6199989318848\",\"61.560001373291\","
				+ "\"61.060001373291\",\"59.9799995422363\",\"58.7099990844727\",\"57.4500007629395\","
				+ "\"58.6100006103516\",\"59.4799995422363\",\"60.5900001525879\",\"61.3899993896484\","
				+ "\"60.8300018310547\",\"61.2000007629395\",\"62.5299987792969\",\"62.75\",\"62.7599983215332\","
				+ "\"62.8199996948242\",\"62.5900001525879\",\"\",";
		String expectedKey1 = "Germany";
		String expectedValue1 = "61.560001373291-61.060001373291-59.9799995422363-58.7099990844727-57.4500007629395-"
				+ "58.6100006103516-59.4799995422363-60.5900001525879-61.3899993896484-60.8300018310547-61.2000007629395-"
				+ "62.5299987792969-62.75-62.7599983215332-62.8199996948242-62.5900001525879-\"\"";
		mapDriver.withInput(new LongWritable(), new Text(record1));
		mapDriver.withOutput(new Text(expectedKey1), new Text(expectedValue1));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		String inputVal1 = "61.560001373291-61.060001373291-59.9799995422363-58.7099990844727-57.4500007629395-"
				+ "58.6100006103516-59.4799995422363-60.5900001525879-61.3899993896484-60.8300018310547-61.2000007629395-"
				+ "62.5299987792969-62.75-62.7599983215332-62.8199996948242-62.5900001525879-\"\"";
		List<Text> values1 = new ArrayList<Text>();
		values1.add(new Text(inputVal1));
		String key1 = "Germany";
		String expectedValue1 = "1.67";
		reduceDriver.withInput(new Text(key1), values1);
		reduceDriver.withOutput(new Text(key1), new Text(expectedValue1));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		String record1 = "\"Germany\",\"DEU\",\"Employment to population ratio, 15+, male (%) (national estimate)\","
				+ "\"SL.EMP.TOTL.SP.MA.NE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"66.2799987792969\",\"66.9300003051758\","
				+ "\"66.6399993896484\",\"67.0400009155273\",\"66.5500030517578\",\"67.3099975585938\","
				+ "\"67.9199981689453\",\"69.0599975585938\",\"68.6900024414063\",\"67.1399993896484\","
				+ "\"65.4199981689453\",\"64.1699981689453\",\"63.7400016784668\",\"62.5400009155273\","
				+ "\"61.5400009155273\",\"61.3400001525879\",\"61.6199989318848\",\"61.560001373291\","
				+ "\"61.060001373291\",\"59.9799995422363\",\"58.7099990844727\",\"57.4500007629395\","
				+ "\"58.6100006103516\",\"59.4799995422363\",\"60.5900001525879\",\"61.3899993896484\","
				+ "\"60.8300018310547\",\"61.2000007629395\",\"62.5299987792969\",\"62.75\",\"62.7599983215332\","
				+ "\"62.8199996948242\",\"62.5900001525879\",\"\",";
		String expectedKeyResult1 = "Germany";
		String expectedValueResult1 = "1.67";
		mapReduceDriver.withInput(new LongWritable(), new Text(record1));
		mapReduceDriver.withOutput(new Text(expectedKeyResult1), new Text(expectedValueResult1));
		mapReduceDriver.runTest();
	}
}
