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

package fire.fileformats.util;

import java.util.regex.Pattern;

import fire.hdfsio.Delete;
import fire.sparkutil.CreateSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import parquet.hadoop.ParquetOutputFormat;
import scala.Tuple2;

public final class ConvertToParquet {
	  private static final Pattern SPACE = Pattern.compile(" ");

	  public static void main(String[] args) throws Exception {

	    if (args.length < 1) {
	      System.err.println("Usage: ConvertToParquet <file>");
	      System.exit(1);
	    }

		  JavaSparkContext ctx = CreateSparkContext.create(args);

	    JavaRDD<String> lines = ctx.textFile(args[0], 1);
		  Delete.deleteFile("aaa");
	    
	    lines.saveAsTextFile("aaa");
	    
	    JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
	        @Override
	        public Tuple2<String, Integer> call(String s) {
	          return new Tuple2<String, Integer>(s, s.length());
	        }
	      });

		  Delete.deleteFile("bbb");
		  pairs.saveAsObjectFile("bbb");

		  Delete.deleteFile("ccc");
		  pairs.saveAsNewAPIHadoopFile("ccc", String.class, Integer.class, ParquetOutputFormat.class);

		  ctx.stop();
	  }
	  

}
