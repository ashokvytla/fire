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

import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.Serializable;
import java.util.Random;

/**
 * Created by jayantshekhar
 */
public class RowStrings implements Serializable {
    public String[] values;

    // creates a RowStrings with the given first value. the rest are calculated randomly
    public RowStrings(int firstval) {
        values = new String[5];

        values[0] = ""+firstval;

        Random r = new Random();
        for (int i=1; i<values.length; i++) {
            values[i] = ""+r.nextFloat();
        }
    }

    public RowStrings(String str) {
        values = str.split(",");
    }

    // assumes that the first point is the label. rest are the dimensions.
    public LabeledPoint toLabeledPoint() {
        double l = Double.parseDouble(values[0]);

        double v[] = new double[values.length-1];
        for (int i=1; i<values.length; i++) {
            v[i-1] = Double.parseDouble(values[i]);
        }

        DenseVector dv = new DenseVector(v);

        LabeledPoint lp = new LabeledPoint(l, dv);

        return lp;
    }
}
