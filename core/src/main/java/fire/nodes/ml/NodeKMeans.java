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

import fire.workflowengine.Node;
import fire.workflowengine.WorkflowContext;
import fire.dataframeutil.DataFrameUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

/**
 * Created by jayantshekhar
 */
public class NodeKMeans extends Node implements Serializable {

    public String clusterColumns = "f1 f2";
    public int maxIter = 10;

    public NodeKMeans() {}

    public NodeKMeans(int i, String nm) {
        super(i, nm);
    }

    public NodeKMeans(int i, String nm, String ccols) {
        super(i, nm);

        clusterColumns = ccols;
    }

    //------------------------------------------------------------------------------------------------------

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {
        workflowContext.out("Executing NodeKMeans : "+id);

        // create vector rdd
        JavaRDD<Vector> vrdd = DataFrameUtil.createVectorRDD(ctx, sqlContext, clusterColumns, df);

        int numClusters = 2;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(vrdd.rdd(), numClusters, numIterations);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        //double WSSSE = clusters.computeCost(vrdd.rdd());
        //workflowContext.out("Within Set Sum of Squared Errors = " + WSSSE);

        super.execute(ctx, sqlContext, workflowContext, df);
    }

    //------------------------------------------------------------------------------------------------------

}
