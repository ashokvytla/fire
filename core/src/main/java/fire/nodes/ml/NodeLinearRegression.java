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
import fire.util.spark.DataFrameUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

/**
 * Created by jayantshekhar
 */
public class NodeLinearRegression extends NodeModeling implements Serializable {

    public int maxIter = 10;
    public double regParam = 0.01;

    public NodeLinearRegression() {}

    public NodeLinearRegression(int i, String nm, String lcol, String pcols) {
        super(i, nm, lcol, pcols);
    }

    //--------------------------------------------------------------------------------------

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext,  DataFrame df) {
        workflowContext.out("Executing NodeLinearRegression : " + id);

        DataFrame lpdf = DataFrameUtil.createLabeledPointsDataFrame(ctx, sqlContext, this.labelColumn, this.predictorColumns, df);

        LinearRegression lr = new LinearRegression()
                .setFeaturesCol("features")
                .setLabelCol("label")
                //.setRegParam(params.regParam)
                //.setElasticNetParam(params.elasticNetParam);
                //.setMaxIter(params.maxIter)
                //.setTol(params.tol)
        ;

        LinearRegressionModel model = lr.fit(lpdf);

        workflowContext.out(model);

        // pass the computed model to the next node if it is a scoring node
        passModel(model);

        super.execute(ctx, sqlContext, workflowContext, df);
    }

    //--------------------------------------------------------------------------------------

}
