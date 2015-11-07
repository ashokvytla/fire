/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fire.misc.sparkloaddatasets;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class DataManipulation {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("DataManipulation");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(args[0]);
		
		//Input data
		//abc,123,102.5,true
		
		//Output data
		//1:abc,2:123,3:102.5,4:true
		//IMPORTANT:even on splitting the new RDD has an array. It's not like each line becomes an independent element in the new RDD
		JavaRDD<String[]> lineSplit = lines.map(new Function<String, String[]>(){
			public String[] call(String s){
				StringBuffer withColumnNames = new StringBuffer();
				String splitString[] = s.split(",");
				for(int i = 0; i < splitString.length;i++){
					withColumnNames.append("column"+(i+1));
					withColumnNames.append(":");
					withColumnNames.append(splitString[i]);
					withColumnNames.append(",");
				}
				return withColumnNames.toString().split(",");

			}
		});
		
		
		//Output data
		//1:abc:String,2:123:Integer,3:102.5:Float,4:true:Boolean
		JavaRDD<String> typeOfColumns = lineSplit.map(new Function<String[],String>(){

			public String call(String[] dataValueList) throws Exception {
				// TODO Auto-generated method stub
				StringBuffer typesOfColumns = new StringBuffer();
				for(int i = 0; i < dataValueList.length;i++){
					String value = dataValueList[i].split(":")[1];
					String typeOfString = checkForType(value);
					typesOfColumns.append(dataValueList[i]);
					typesOfColumns.append(":");
					typesOfColumns.append(typeOfString);
					typesOfColumns.append(",");
				}
				typesOfColumns.setLength(typesOfColumns.length()-1);
				return typesOfColumns.toString();
				
			}
			
		});

		
		//Output
		//1:abc:String
		//2:123:Integer
		//3:102.5:Float
		//4:true:Boolean
		JavaRDD<String> columns = typeOfColumns.flatMap(new FlatMapFunction<String, String>() {
				
			public Iterable<String> call(String x) {
					return Arrays.asList(x.split(","));
			}
		});
		
		//create a pair of column type 
		//String,1
		//Integer,1
		JavaPairRDD<String, Integer> pairs = columns.mapToPair(new PairFunction<String, String, Integer>() {

			public Tuple2<String, Integer> call(String inputLine) throws Exception {
				// TODO Auto-generated method stub
			
					return new Tuple2<String, Integer>(inputLine.split(":")[2], 1);
	
			}
		});
		
	
		//find count of each column type
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) { return x + y;};
		});
		

//		counts.persist(StorageLevel.MEMORY_AND_DISK());
		counts.saveAsTextFile(args[1]);

	}
	
	//  Coded for the following data types
	//	boolean
	//	integer
	//	long
	//	float
	//	double
	//	String
	
	public static String checkForType(String inputValue){
		  String type = checkForBoolean(inputValue);
		  if(type.compareTo("boolean")!=0){
			  type = checkForInteger(inputValue);
			  if(type.compareTo("Integer")!=0){
				  type = checkForLong(inputValue);
				  if(type.compareTo("Long")!=0){
					  //add code for float and double
					  type =checkForFloat(inputValue);
					  if(type.compareTo("Float")!=0){
						  type=checkForDouble(inputValue);
						  if(type.compareTo("Double")!=0){
							  return "String";
						  }
					  }
					 
				  }
			  }
		  }
		 
			return type;
	}

	private static String checkForDouble(String inputValue) {
		// TODO Auto-generated method stub
		try{
			Double.parseDouble(inputValue);
			return "Double";
		}catch(NumberFormatException e){
//			e.printStackTrace();
			return "notdouble";
		}
	
	}

	private static String checkForFloat(String inputValue) {
		// TODO Auto-generated method stub
		try{
			Float.parseFloat(inputValue);
			return "Float";
		}catch(NumberFormatException e){
//			e.printStackTrace();
			return "notfloat";
		}
		
		
	}

	private static String checkForLong(String inputValue) {
	// TODO Auto-generated method stub
		try{
			Long.parseLong(inputValue);
				return "Long";
			}catch(NumberFormatException e){
//			e.printStackTrace();
			return "notlong";
		}
		

}

	private static String checkForInteger(String inputValue) {
		// TODO Auto-generated method stub
		try{
			Integer.parseInt(inputValue);
			return "Integer";
		}catch(NumberFormatException e){
//			e.printStackTrace();
			return "notinteger";
		}
		

	}

	private static String checkForBoolean(String inputValue) {
		// TODO Auto-generated method stub
		try{
			if((inputValue.compareToIgnoreCase("true")==0)|| (inputValue.compareToIgnoreCase("false")==0)){
				return "boolean";
			}else{
				return "notboolean";
			}
			
		}catch(Exception e){
//			e.printStackTrace();
			return "notboolean";
		}
		
		
	}

}
