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
import fire.dataframeutil.DataFrameUtil;
import fire.workflowengine.Node;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

/**
 * Created by jayantshekhar
 */
public class NodeStandardScaler extends Node implements Serializable {

    public boolean withMean = true;
    public boolean withStd = true;

    public NodeStandardScaler(int i, String nm) {
        super(i, nm);
    }

    public NodeStandardScaler()
    {

    }

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {

        workflowContext.out("Executing NodeStandardScaler : "+id);

        JavaRDD<Vector> rdd = DataFrameUtil.createVectorRDD(ctx, sqlContext, "f1 f2", df);

        org.apache.spark.mllib.feature.StandardScaler standardScaler = new org.apache.spark.mllib.feature.StandardScaler(withMean, withStd);
        org.apache.spark.mllib.feature.StandardScalerModel model = standardScaler.fit(rdd.rdd());

        JavaRDD<Vector> ssrdd = model.transform(rdd);

        // Apply the schema to the RDD.
        DataFrame tdf = DataFrameUtil.createDataFrame(ctx, sqlContext, ssrdd, "f1 f2", "numeric numeric");


        /***
        ParamMap paramMap = new ParamMap();
        paramMap.put();
        StandardScaler standardScaler = new StandardScaler();
        standardScaler.

        // fit the dataframe to the standard scaler model
        StandardScalerModel model = standardScaler.fit(df, paramMap);

        // transform the dataframe using the model
        DataFrame newdf = model.transform(df);
         ***/

        super.execute(ctx, sqlContext, workflowContext, tdf);
    }

}
