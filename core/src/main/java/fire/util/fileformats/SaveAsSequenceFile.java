package fire.util.fileformats;

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

import fire.util.hdfsio.Delete;
import fire.util.spark.CreateSparkContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public final class SaveAsSequenceFile {

    public static class ConvertToWritableTypes implements PairFunction<Tuple2<String, Integer>, Text, IntWritable> {
        public Tuple2<Text, IntWritable> call(Tuple2<String, Integer> record) {
            return new Tuple2(new Text(record._1()), new IntWritable(record._2()));
        }
    }

    public static class ConvertToNativeTypes implements PairFunction<Tuple2<Text, IntWritable>, String, Integer> {
        public Tuple2<String, Integer> call(Tuple2<Text, IntWritable> record) {
            return new Tuple2(record._1().toString(), record._2().get());
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: SaveAsSequenceFile <file>");
            System.exit(1);
        }

        JavaSparkContext ctx = CreateSparkContext.create(args);

        JavaRDD<String> file1 = ctx.textFile(args[0], 1);

        JavaPairRDD<String, Integer> ones = file1.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<Text, IntWritable> result = ones.mapToPair(new ConvertToWritableTypes());

        Delete.deleteFile("sequencefile");

        // save as sequence file
        result.saveAsHadoopFile("sequencefile", Text.class, IntWritable.class, SequenceFileOutputFormat.class);

        // read back the sequence file
        JavaPairRDD<Text, IntWritable> input = ctx.sequenceFile("sequencefile", Text.class, IntWritable.class);
        JavaPairRDD<String, Integer> input_1 = input.mapToPair(new ConvertToNativeTypes());
        List<Tuple2<String, Integer>> resultList = input_1.collect();
        for (Tuple2<String, Integer> record : resultList) {
            System.out.println(record);
        }

        ctx.stop();
    }
}
