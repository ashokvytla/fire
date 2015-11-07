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

package fire.nodes.compactor;

import fire.sparkutil.CreateSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar
 */
public class CompactTextFiles {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: CompactTextFiles <file>");
            System.exit(1);
        }

        JavaSparkContext ctx = CreateSparkContext.create(args);

        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        lines.saveAsTextFile("compactedfiles");

        ctx.stop();
    }
}
