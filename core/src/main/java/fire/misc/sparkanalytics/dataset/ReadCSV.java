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

import fire.sparkutil.CreateSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by jayantshekhar
 */
public class ReadCSV {

    private static final Pattern SPACE = Pattern.compile(",");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: ReadCSV <file>");
            System.exit(1);
        }

        JavaSparkContext ctx = CreateSparkContext.create(args);

        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        JavaRDD<RowStrings> rowstrings = lines.map(new Function<String, RowStrings>() {
            @Override
            public RowStrings call(String s) throws Exception {
                RowStrings rowStrings = new RowStrings(s);
                return rowStrings;
            }
        });


        List<RowStrings> list = rowstrings.collect();

        for (RowStrings rowStrings : list) {
            System.out.println(rowStrings.values[0]);
        }

        ctx.stop();
    }
}
