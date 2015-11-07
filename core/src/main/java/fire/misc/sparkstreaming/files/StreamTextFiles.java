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

package fire.misc.sparkstreaming.files;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by jayantshekhar
 */
public class StreamTextFiles {
    private StreamTextFiles() {
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: StreamTextFiles <in> <out>");
            System.exit(1);
        }

        Duration batchInterval = new Duration(20000);
        SparkConf sparkConf = new SparkConf().setAppName("JavaFlumeEventCount").setMaster("local");
        sparkConf.set("spark.broadcast.compress", "false");
        sparkConf.set("spark.shuffle.compress", "false");

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);
        JavaDStream<String> textStream = ssc.textFileStream(args[0]);

        textStream.count();

        textStream.count().map(new Function<Long, String>() {
            @Override
            public String call(Long in) {
                return "Received " + in + " events.";
            }
        }).print();

        textStream.dstream().saveAsTextFiles("aaa", "bbb");

        ssc.start();
        ssc.awaitTermination();
    }
}