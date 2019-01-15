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
	 * These 12 MRUnit test methods will test map-reduce on every row of the csv file that 
	 * will contribute to the overall output of the hadoop job.
	 */
	
	/*
	@Test
	public void testMapper() {
		String record1 = "\"United States\",\"USA\",\"School enrollment, primary (gross), "
				+ "gender parity index (GPI)\",\"SE.ENR.PRIM.FM.ZS\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"1.02214\",\"1.01313\",\"0.99526\",\"1.00909\",\"1.0163\","
				+ "\"1.01332\",\"0.99819\",\"\",\"\",\"1.00004\",\"0.98624\",\"\",\"0.98137\","
				+ "\"0.99085\",\"0.9906\",\"0.98955\",\"\",\"1.02948\",\"1.03086\",\"0.98671\","
				+ "\"0.99986\",\"1.01145\",\"1.00518\",\"0.99803\",\"0.99142\",\"1.0097\","
				+ "\"0.99934\",\"1.00462\",\"1.00656\",\"0.99171\",\"1.00511\",\"0.98433\","
				+ "\"0.99002\",\"0.99405\",\"0.99787\",\"\",";
		String expectedKey1 = "School enrollment - primary (gross) - gender parity index (GPI)";
		String expectedValue1 = "0.98671-0.99986-1.01145-1.00518-0.99803-0.99142-1.0097-0.99934-"
				+ "1.00462-1.00656-0.99171-1.00511-0.98433-0.99002-0.99405-0.99787-\"\"";
		mapDriver.withInput(new LongWritable(), new Text(record1));
		mapDriver.withOutput(new Text(expectedKey1), new Text(expectedValue1));
		mapDriver.runTest();
	}

	@Test
	public void testMapperAgain() {
		String record2 = "\"United States\",\"USA\",\"School enrollment, primary and secondary (gross), "
				+ "gender parity index (GPI)\",\"SE.ENR.PRSC.FM.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"1.032\","
				+ "\"1.02738\",\"1.01755\",\"1.02096\",\"1.01995\",\"1.0229\",\"1.01339\",\"\",\"\","
				+ "\"1.00497\",\"1.00659\",\"\",\"0.99642\",\"0.99868\",\"1.00831\",\"0.99989\",\"\","
				+ "\"1.03635\",\"\",\"1.00012\",\"1.0082\",\"1.00371\",\"1.00663\",\"1.01326\",\"1.00999\","
				+ "\"1.00745\",\"1.00754\",\"1.00381\",\"1.01385\",\"1.00479\",\"1.00869\",\"0.9984\","
				+ "\"0.99954\",\"1.00609\",\"\",\"\",";
		String expectedKey2 = "School enrollment - primary and secondary (gross) - gender parity index (GPI)";
		String expectedValue2 = "1.00012-1.0082-1.00371-1.00663-1.01326-1.00999-1.00745-1.00754-"
				+ "1.00381-1.01385-1.00479-1.00869-0.9984-0.99954-1.00609-\"\"-\"\"";
		mapDriver.withInput(new LongWritable(), new Text(record2));
		mapDriver.withOutput(new Text(expectedKey2), new Text(expectedValue2));
		mapDriver.runTest();
	}

	@Test
	public void testMapperThirdTime() {
		String record3 = "\"United States\",\"USA\",\"School enrollment, secondary (gross), "
				+ "gender parity index (GPI)\",\"SE.ENR.SECO.FM.ZS\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.56187\",\"\",\"\",\"\",\"\",\"\",\"1.00682\","
				+ "\"\",\"\",\"1.04144\",\"1.04075\",\"1.03867\",\"1.03188\",\"1.02314\",\"1.03212\","
				+ "\"1.02873\",\"\",\"\",\"1.00994\",\"1.0298\",\"\",\"1.01285\",\"1.00681\",\"1.02787\","
				+ "\"1.01102\",\"\",\"1.04388\",\"\",\"1.01496\",\"1.01716\",\"0.99524\",\"1.00791\","
				+ "\"1.02872\",\"1.02859\",\"1.00481\",\"1.01537\",\"1.00263\",\"1.02085\",\"1.01777\","
				+ "\"1.01195\",\"1.0125\",\"1.00905\",\"1.0183\",\"\",\"\",";
		String expectedKey3 = "School enrollment - secondary (gross) - gender parity index (GPI)";
		String expectedValue3 = "1.01496-1.01716-0.99524-1.00791-1.02872-1.02859-1.00481-1.01537-"
				+ "1.00263-1.02085-1.01777-1.01195-1.0125-1.00905-1.0183-\"\"-\"\"";
		mapDriver.withInput(new LongWritable(), new Text(record3));
		mapDriver.withOutput(new Text(expectedKey3), new Text(expectedValue3));
		mapDriver.runTest();
	}
	
	@Test
	public void testMapperFourthTime() {
		String record4 = "\"United States\",\"USA\",\"School enrollment, tertiary (gross), "
				+ "gender parity index (GPI)\",\"SE.ENR.TERT.FM.ZS\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"0.68491\",\"0.71732\",\"0.77382\",\"0.80667\","
				+ "\"0.84129\",\"0.843\",\"0.92271\",\"0.97926\",\"1.02791\",\"1.0715\",\"1.10154\","
				+ "\"1.11647\",\"1.10805\",\"1.12\",\"1.14287\",\"1.16682\",\"1.19576\",\"1.23076\","
				+ "\"1.25823\",\"1.27196\",\"1.28147\",\"\",\"1.29102\",\"1.29262\",\"1.30882\","
				+ "\"1.32194\",\"\",\"1.33569\",\"1.33622\",\"1.34118\",\"1.346\",\"1.36367\","
				+ "\"1.3809\",\"1.40626\",\"1.41924\",\"1.43254\",\"1.42824\",\"1.41559\",\"1.41207\","
				+ "\"1.40825\",\"1.40311\",\"1.40738\",\"1.38494\",\"1.37069\",\"1.36754\",\"\",";
		String expectedKey4 = "School enrollment - tertiary (gross) - gender parity index (GPI)";
		String expectedValue4 = "1.34118-1.346-1.36367-1.3809-1.40626-1.41924-1.43254-1.42824-"
				+ "1.41559-1.41207-1.40825-1.40311-1.40738-1.38494-1.37069-1.36754-\"\"";
		mapDriver.withInput(new LongWritable(), new Text(record4));
		mapDriver.withOutput(new Text(expectedKey4), new Text(expectedValue4));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		String inputVal1 = "0.98671-0.99986-1.01145-1.00518-0.99803-0.99142-1.0097-0.99934-"
				+ "1.00462-1.00656-0.99171-1.00511-0.98433-0.99002-0.99405-0.99787-\"\"";
		List<Text> values1 = new ArrayList<Text>();
		values1.add(new Text(inputVal1));
		String key1 = "School enrollment - primary (gross) - gender parity index (GPI)";
		String expectedValue1 = "1.324,1.152,-0.622,-0.714,-0.665,1.827,-1.031,0.527,0.193,"
				+ "-1.486,1.342,-2.089,0.576,0.406,0.384,Total: 16";
		reduceDriver.withInput(new Text(key1), values1);
		reduceDriver.withOutput(new Text(key1), new Text(expectedValue1));
		reduceDriver.runTest();
	}

	@Test
	public void testReducerAgain() {
		String inputVal2 = "1.00012-1.0082-1.00371-1.00663-1.01326-1.00999-1.00745-1.00754-"
				+ "1.00381-1.01385-1.00479-1.00869-0.9984-0.99954-1.00609-\"\"-\"\"";
		List<Text> values2 = new ArrayList<Text>();
		values2.add(new Text(inputVal2));
		String key2 = "School enrollment - primary and secondary (gross) - gender parity index (GPI)";
		String expectedValue2 = "0.805,-0.446,0.290,0.656,-0.323,-0.252,0.009,-0.371,0.995,-0.898,"
				+ "0.387,-1.025,0.114,0.653,Total: 15";
		//average absolute percent change is 0.482 % (found by adding the absolute values of each relative percent difference and then diving by the total number of values)
		reduceDriver.withInput(new Text(key2), values2);
		reduceDriver.withOutput(new Text(key2), new Text(expectedValue2));
		reduceDriver.runTest();
	}

	@Test
	public void testReducerThirdTime() {
		String inputVal3 = "1.01496-1.01716-0.99524-1.00791-1.02872-1.02859-1.00481-1.01537-"
				+ "1.00263-1.02085-1.01777-1.01195-1.0125-1.00905-1.0183-\"\"-\"\"";
		List<Text> values3 = new ArrayList<Text>();
		values3.add(new Text(inputVal3));
		String key3 = "School enrollment - secondary (gross) - gender parity index (GPI)";
		String expectedValue3 = "0.217,-2.178,1.265,2.044,-0.013,-2.339,1.045,-1.263,1.801,"
				+ "-0.302,-0.573,0.054,-0.341,0.913,Total: 15";
		reduceDriver.withInput(new Text(key3), values3);
		reduceDriver.withOutput(new Text(key3), new Text(expectedValue3));
		reduceDriver.runTest();
	}
	
	@Test
	public void testReducerFourthTime() {
		String inputVal4 = "1.34118-1.346-1.36367-1.3809-1.40626-1.41924-1.43254-1.42824-"
				+ "1.41559-1.41207-1.40825-1.40311-1.40738-1.38494-1.37069-1.36754-\"\"";
		List<Text> values4 = new ArrayList<Text>();
		values4.add(new Text(inputVal4));
		String key4 = "School enrollment - tertiary (gross) - gender parity index (GPI)";
		String expectedValue4 = "0.359,1.304,1.256,1.820,0.919,0.933,-0.301,-0.890,-0.249,"
				+ "-0.271,-0.366,0.304,-1.607,-1.034,-0.230,Total: 16";
		reduceDriver.withInput(new Text(key4), values4);
		reduceDriver.withOutput(new Text(key4), new Text(expectedValue4));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		String record1 = "\"United States\",\"USA\",\"School enrollment, primary (gross), "
				+ "gender parity index (GPI)\",\"SE.ENR.PRIM.FM.ZS\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"1.02214\",\"1.01313\",\"0.99526\",\"1.00909\",\"1.0163\","
				+ "\"1.01332\",\"0.99819\",\"\",\"\",\"1.00004\",\"0.98624\",\"\",\"0.98137\","
				+ "\"0.99085\",\"0.9906\",\"0.98955\",\"\",\"1.02948\",\"1.03086\",\"0.98671\","
				+ "\"0.99986\",\"1.01145\",\"1.00518\",\"0.99803\",\"0.99142\",\"1.0097\","
				+ "\"0.99934\",\"1.00462\",\"1.00656\",\"0.99171\",\"1.00511\",\"0.98433\","
				+ "\"0.99002\",\"0.99405\",\"0.99787\",\"\",";
		String expectedKeyResult1 = "School enrollment - primary (gross) - gender parity index (GPI)";
		String expectedValueResult1 = "1.324,1.152,-0.622,-0.714,-0.665,1.827,-1.031,0.527,0.193,"
				+ "-1.486,1.342,-2.089,0.576,0.406,0.384,Total: 16";
		mapReduceDriver.withInput(new LongWritable(), new Text(record1));
		mapReduceDriver.withOutput(new Text(expectedKeyResult1), new Text(expectedValueResult1));
		mapReduceDriver.runTest();
	}
	
	@Test
	public void testMapReduceAgain() {
		String record2 = "\"United States\",\"USA\",\"School enrollment, primary and secondary (gross), "
				+ "gender parity index (GPI)\",\"SE.ENR.PRSC.FM.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"1.032\","
				+ "\"1.02738\",\"1.01755\",\"1.02096\",\"1.01995\",\"1.0229\",\"1.01339\",\"\",\"\","
				+ "\"1.00497\",\"1.00659\",\"\",\"0.99642\",\"0.99868\",\"1.00831\",\"0.99989\",\"\","
				+ "\"1.03635\",\"\",\"1.00012\",\"1.0082\",\"1.00371\",\"1.00663\",\"1.01326\",\"1.00999\","
				+ "\"1.00745\",\"1.00754\",\"1.00381\",\"1.01385\",\"1.00479\",\"1.00869\",\"0.9984\","
				+ "\"0.99954\",\"1.00609\",\"\",\"\",";
		String expectedKeyResult2 = "School enrollment - primary and secondary (gross) - gender parity index (GPI)";
		String expectedValueResult2 = "0.805,-0.446,0.290,0.656,-0.323,-0.252,0.009,-0.371,0.995,-0.898,"
				+ "0.387,-1.025,0.114,0.653,Total: 15";
		mapReduceDriver.withInput(new LongWritable(), new Text(record2));
		mapReduceDriver.withOutput(new Text(expectedKeyResult2), new Text(expectedValueResult2));
		mapReduceDriver.runTest();
	}
	
	@Test
	public void testMapReduceThirdTime() {
		String record3 = "\"United States\",\"USA\",\"School enrollment, secondary (gross), "
				+ "gender parity index (GPI)\",\"SE.ENR.SECO.FM.ZS\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.56187\",\"\",\"\",\"\",\"\",\"\","
				+ "\"1.00682\",\"\",\"\",\"1.04144\",\"1.04075\",\"1.03867\",\"1.03188\",\"1.02314\","
				+ "\"1.03212\",\"1.02873\",\"\",\"\",\"1.00994\",\"1.0298\",\"\",\"1.01285\","
				+ "\"1.00681\",\"1.02787\",\"1.01102\",\"\",\"1.04388\",\"\",\"1.01496\",\"1.01716\","
				+ "\"0.99524\",\"1.00791\",\"1.02872\",\"1.02859\",\"1.00481\",\"1.01537\",\"1.00263\","
				+ "\"1.02085\",\"1.01777\",\"1.01195\",\"1.0125\",\"1.00905\",\"1.0183\",\"\",\"\",";
		String expectedKeyResult3 = "School enrollment - secondary (gross) - gender parity index (GPI)";
		String expectedValueResult3 = "0.217,-2.178,1.265,2.044,-0.013,-2.339,1.045,-1.263,1.801,-0.302,"
				+ "-0.573,0.054,-0.341,0.913,Total: 15";
		mapReduceDriver.withInput(new LongWritable(), new Text(record3));
		mapReduceDriver.withOutput(new Text(expectedKeyResult3), new Text(expectedValueResult3));
		mapReduceDriver.runTest();
	}
	
	@Test
	public void testMapReduceFourthTime() {
		String record4 = "\"United States\",\"USA\",\"School enrollment, tertiary (gross), "
				+ "gender parity index (GPI)\",\"SE.ENR.TERT.FM.ZS\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"0.68491\",\"0.71732\",\"0.77382\",\"0.80667\","
				+ "\"0.84129\",\"0.843\",\"0.92271\",\"0.97926\",\"1.02791\",\"1.0715\",\"1.10154\","
				+ "\"1.11647\",\"1.10805\",\"1.12\",\"1.14287\",\"1.16682\",\"1.19576\",\"1.23076\","
				+ "\"1.25823\",\"1.27196\",\"1.28147\",\"\",\"1.29102\",\"1.29262\",\"1.30882\","
				+ "\"1.32194\",\"\",\"1.33569\",\"1.33622\",\"1.34118\",\"1.346\",\"1.36367\","
				+ "\"1.3809\",\"1.40626\",\"1.41924\",\"1.43254\",\"1.42824\",\"1.41559\",\"1.41207\","
				+ "\"1.40825\",\"1.40311\",\"1.40738\",\"1.38494\",\"1.37069\",\"1.36754\",\"\",";
		String expectedKeyResult4 = "School enrollment - tertiary (gross) - gender parity index (GPI)";
		String expectedValueResult4 = "0.359,1.304,1.256,1.820,0.919,0.933,-0.301,-0.890,-0.249,-0.271,"
				+ "-0.366,0.304,-1.607,-1.034,-0.230,Total: 16";
		mapReduceDriver.withInput(new LongWritable(), new Text(record4));
		mapReduceDriver.withOutput(new Text(expectedKeyResult4), new Text(expectedValueResult4));
		mapReduceDriver.runTest();
	}
	*/
}
