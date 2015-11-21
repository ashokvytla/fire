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


package fire.util.fileformats.csv;

import au.com.bytecode.opencsv.CSVReader;
import fire.util.spark.CreateSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.io.StringReader;
import java.util.List;

public class ReadCSV {

    public static class ParseLine implements FlatMapFunction<Tuple2<String, String>, String[]> {
        public Iterable<String[]> call(Tuple2<String, String> file) throws Exception {
            CSVReader reader = new CSVReader(new StringReader(file._2()));
            return reader.readAll();
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: ReadCSV <file>");
            System.exit(1);
        }

        JavaSparkContext ctx = CreateSparkContext.create(args);

        JavaPairRDD<String, String> csvData = ctx.wholeTextFiles(args[0]);
        JavaRDD<String[]> rdd = csvData.flatMap(new ParseLine());

        List<String[]> result = rdd.collect();

        for (int i = 0; i < result.size(); i++) {
            String[] arr = result.get(i);
            for (int j = 0; j<arr.length; j++) {
                System.out.print(arr[j] + " ");
            }
            System.out.println("");
        }
    }
}