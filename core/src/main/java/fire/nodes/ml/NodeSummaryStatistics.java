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

package fire.nodes.ml;

import fire.workflowengine.WorkflowContext;
import fire.workflowengine.Node;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

/**
 * Created by jayantshekhar
 */
public class NodeSummaryStatistics extends Node implements Serializable {

    public NodeSummaryStatistics(int i, String nm) {
        super(i, nm);
    }

    public NodeSummaryStatistics()
    {

    }

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {

        System.out.println("Executing NodeSummaryStatistics : "+id);


        /***
        JavaRDD<Vector> rdd = DataFrameUtil.createVectorRDD(ctx, sqlContext, df);

        List<Vector> list = rdd.collect();

        for (Vector vector : list) {
            System.out.println(vector.toString());
        }

        // Compute column summary statistics.
        MultivariateStatisticalSummary summary = Statistics.colStats(rdd.rdd());
        System.out.println(summary.mean()); // a dense vector containing the mean value for each column
        System.out.println(summary.variance()); // column-wise variance
        System.out.println(summary.numNonzeros()); // number of nonzeros in each column
         ***/

        super.execute(ctx, sqlContext, workflowContext, df);
    }

}
