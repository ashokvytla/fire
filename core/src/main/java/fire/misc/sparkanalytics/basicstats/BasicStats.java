/**
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

package fire.misc.sparkanalytics.basicstats;

import fire.misc.sparkanalytics.dataset.CreateDataSet;
import fire.misc.sparkanalytics.dataset.RowStrings;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.DoubleRDDFunctions;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;

/**
 * Created by jayantshekhar
 */
public class BasicStats {

    public static BasicStatsResult compute(JavaRDD<RowStrings> rdd, final int colIdx) {

        JavaRDD<Object> r = CreateDataSet.toDouble(rdd, 2);

        DoubleRDDFunctions f = new DoubleRDDFunctions(r.rdd());

        StatCounter sc = f.stats();

        BasicStatsResult result = new BasicStatsResult();

        result.mean = sc.mean();
        result.stdev = sc.stdev();
        result.variance = sc.variance();
        result.max = sc.max();
        result.min = sc.min();
        result.sum = sc.sum();

        result.count = sc.count();

        return result;
    }

    public static Histogram histogram(JavaRDD<RowStrings> rdd, final int colIdx) {

        JavaRDD<Object> r = rdd.map(new Function<RowStrings, Object>() {
            public Object call(RowStrings d) {
                return Double.parseDouble(d.values[colIdx]);
            }
        });

        DoubleRDDFunctions f = new DoubleRDDFunctions(r.rdd());

        Tuple2<double[], long[]> h = f.histogram(5);

        Histogram result = new Histogram();
        result.x = h._2();
        result.y = h._1();

        return result;
    }
}
