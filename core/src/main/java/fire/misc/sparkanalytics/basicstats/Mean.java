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

import fire.misc.sparkanalytics.dataset.RowStrings;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.*;
import org.apache.spark.api.java.function.*;

/**
 * Created by jayantshekhar
 */
public class Mean {

    public static double mean(JavaRDD<RowStrings> rdd, final int colIdx) {

        JavaRDD<Object> r = rdd.map(new Function<RowStrings, Object>() {
            public Object call(RowStrings d) {
                return Double.parseDouble(d.values[colIdx]);
            }
        });

        DoubleRDDFunctions f = new DoubleRDDFunctions(r.rdd());

        return f.mean();
    }

}
