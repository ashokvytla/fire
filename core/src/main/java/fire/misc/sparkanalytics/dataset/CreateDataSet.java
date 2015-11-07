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

package fire.misc.sparkanalytics.dataset;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jayantshekhar
 */
public class CreateDataSet {

    public static JavaRDD<Integer> createIntegerDataset(JavaSparkContext ctx) {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = ctx.parallelize(data);

        return rdd;
    }

    public static JavaRDD<Double> createDoubleDataset(JavaSparkContext ctx) {
        List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);
        JavaRDD<Double> rdd = ctx.parallelize(data);

        return rdd;
    }

    public static JavaRDD<RowStrings> createRowStringsDataset(JavaSparkContext ctx) {
        List<RowStrings> data = new ArrayList<RowStrings>();

        for (int i=0; i<20; i++) {
            data.add(new RowStrings(i%2));
        }

        JavaRDD<RowStrings> rdd = ctx.parallelize(data);

        return rdd;
    }

    public static JavaRDD<Object> toDouble(JavaRDD<RowStrings> rdd, final int colIdx) {
        JavaRDD<Object> r = rdd.map(new Function<RowStrings, Object>() {
            public Object call(RowStrings d) {
                return Double.parseDouble(d.values[colIdx]);
            }
        });

        return r;
    }

    public static JavaRDD<LabeledPoint> toLabeledPoint(JavaRDD<RowStrings> rdd) {
        JavaRDD<LabeledPoint> r = rdd.map(new Function<RowStrings, LabeledPoint>() {
            public LabeledPoint call(RowStrings d) {
                LabeledPoint lp = d.toLabeledPoint();

                return lp;
            }
        });

        return r;
    }
}
