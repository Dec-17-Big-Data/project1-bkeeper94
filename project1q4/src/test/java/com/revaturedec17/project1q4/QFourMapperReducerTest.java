package com.revaturedec17.project1q4;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class QFourMapperReducerTest {
	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		QFourMapper mapper = new QFourMapper();
		QFourReducer reducer = new QFourReducer();
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
		String record1 = "\"Germany\",\"DEU\",\"Employment to population ratio, 15+, female (%) (national estimate)\","
				+ "\"SL.EMP.TOTL.SP.FE.NE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"37.0900001525879\",\"37.3499984741211\","
				+ "\"37.6399993896484\",\"38.1399993896484\",\"38.1699981689453\",\"38.7799987792969\","
				+ "\"39.3499984741211\",\"42.5800018310547\",\"45\",\"43.9300003051758\",\"43.1699981689453\","
				+ "\"42.8499984741211\",\"43.0900001525879\",\"43.2599983215332\",\"43.0800018310547\","
				+ "\"43.3600006103516\",\"44.439998626709\",\"44.8199996948242\",\"45.4599990844727\","
				+ "\"45.310001373291\",\"45.1199989318848\",\"44.4500007629395\",\"45.5400009155273\","
				+ "\"46.689998626709\",\"47.7999992370605\",\"48.5\",\"49.060001373291\",\"49.6100006103516\","
				+ "\"50.9900016784668\",\"51.2299995422363\",\"51.9000015258789\",\"52.2299995422363\","
				+ "\"52.4000015258789\",\"\",";
		String expectedKey1 = "Germany";
		String expectedValue1 = "44.8199996948242-45.4599990844727-45.310001373291-45.1199989318848-44.4500007629395-"
				+ "45.5400009155273-46.689998626709-47.7999992370605-48.5-49.060001373291-49.6100006103516-"
				+ "50.9900016784668-51.2299995422363-51.9000015258789-52.2299995422363-52.4000015258789-\"\"";
		mapDriver.withInput(new LongWritable(), new Text(record1));
		mapDriver.withOutput(new Text(expectedKey1), new Text(expectedValue1));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		String inputVal1 = "44.8199996948242-45.4599990844727-45.310001373291-45.1199989318848-44.4500007629395-"
				+ "45.5400009155273-46.689998626709-47.7999992370605-48.5-49.060001373291-49.6100006103516-"
				+ "50.9900016784668-51.2299995422363-51.9000015258789-52.2299995422363-52.4000015258789-\"\"";
		List<Text> values1 = new ArrayList<Text>();
		values1.add(new Text(inputVal1));
		String key1 = "Germany";
		String expectedValue1 = "16.91";
		reduceDriver.withInput(new Text(key1), values1);
		reduceDriver.withOutput(new Text(key1), new Text(expectedValue1));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		String record1 = "\"Germany\",\"DEU\",\"Employment to population ratio, 15+, female (%) (national estimate)\","
				+ "\"SL.EMP.TOTL.SP.FE.NE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"37.0900001525879\",\"37.3499984741211\","
				+ "\"37.6399993896484\",\"38.1399993896484\",\"38.1699981689453\",\"38.7799987792969\","
				+ "\"39.3499984741211\",\"42.5800018310547\",\"45\",\"43.9300003051758\",\"43.1699981689453\","
				+ "\"42.8499984741211\",\"43.0900001525879\",\"43.2599983215332\",\"43.0800018310547\","
				+ "\"43.3600006103516\",\"44.439998626709\",\"44.8199996948242\",\"45.4599990844727\","
				+ "\"45.310001373291\",\"45.1199989318848\",\"44.4500007629395\",\"45.5400009155273\","
				+ "\"46.689998626709\",\"47.7999992370605\",\"48.5\",\"49.060001373291\",\"49.6100006103516\","
				+ "\"50.9900016784668\",\"51.2299995422363\",\"51.9000015258789\",\"52.2299995422363\","
				+ "\"52.4000015258789\",\"\",";
		String expectedKeyResult1 = "Germany";
		String expectedValueResult1 = "16.91";
		mapReduceDriver.withInput(new LongWritable(), new Text(record1));
		mapReduceDriver.withOutput(new Text(expectedKeyResult1), new Text(expectedValueResult1));
		mapReduceDriver.runTest();
	}
}
