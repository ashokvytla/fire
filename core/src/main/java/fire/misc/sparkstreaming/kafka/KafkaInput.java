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


package fire.misc.sparkstreaming.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.kafka.*;

public final class KafkaInput {
    public static void main(String[] args) throws Exception {
        String zkQuorum = args[0];
        String group = args[1];
        SparkConf conf = new SparkConf().setAppName("KafkaInput");
        // Create a StreamingContext with a 1 second batch size
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));
        Map<String, Integer> topics = new HashMap<String, Integer>();
        topics.put("pandas", 1);
        JavaPairDStream<String, String> input = KafkaUtils.createStream(jssc, zkQuorum, group, topics);
        input.print();
        // start our streaming context and wait for it to "finish"
        jssc.start();
        // Wait for 10 seconds then exit. To run forever call without a timeout
        jssc.awaitTermination(10000);
        // Stop the streaming context
        jssc.stop();
    }
}