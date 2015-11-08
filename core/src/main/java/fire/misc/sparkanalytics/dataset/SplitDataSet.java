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
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.List;

/**
 * Created by jayantshekhar
 */
public class SplitDataSet {

    public JavaRDD<LabeledPoint> training;
    public JavaRDD<LabeledPoint> test;

    // trainingFraction = .6 for 60%
    public static SplitDataSet split(JavaRDD<LabeledPoint> rdd, double trainingFraction) {

        SplitDataSet dataset = new SplitDataSet();

        // Split initial RDD into two... [60% training data, 40% testing data].
        dataset.training = rdd.sample(false, trainingFraction, 11L);

        dataset.test = rdd.subtract(dataset.training);

        return dataset;
    }

    public void print() {
        System.out.println("Printing the training dataset");
        print(training);

        System.out.println("Printing the test dataset");
        print(test);
    }

    public void print(JavaRDD<LabeledPoint> rdd) {
        List<LabeledPoint> list = rdd.collect();

        for (LabeledPoint lp : list) {
            System.out.println(lp.toString());
        }
    }
}
