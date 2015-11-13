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

import fire.dataframeutil.DataFrameUtil;
import fire.workflowengine.Node;
import fire.workflowengine.NodeDataset;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.*;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

/**
 * Created by jayantshekhar
 */
public class NodeLinearRegressionWithSGD extends NodeDataset implements Serializable {

    public String labelColumn = "label";
    public String predictorColumns = "f1 f2";
    public int maxIter = 10;
    public double regParam = 0.01;

    public NodeLinearRegressionWithSGD() {}

    public NodeLinearRegressionWithSGD(int i, String nm) {
        super(i, nm);
    }

    //--------------------------------------------------------------------------------------

    public NodeLinearRegressionWithSGD(int i, String nm, String lcol, String pcols) {
        super(i, nm);

        labelColumn = lcol;
        predictorColumns = pcols;
    }

    //--------------------------------------------------------------------------------------

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {
        workflowContext.out("Executing NodeLinearRegressionWithSGD : " + id);

        DataFrame lpdf = DataFrameUtil.createLabeledPointsDataFrame(ctx, sqlContext, this.labelColumn, this.predictorColumns, df);

        // output the schema
        workflowContext.outSchema(lpdf);

        linearRegressionWithSGD(ctx, sqlContext, workflowContext, df);

        super.execute(ctx, sqlContext, workflowContext, df);
    }

    //--------------------------------------------------------------------------------------

    private void linearRegressionWithSGD(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {

        LinearRegressionWithSGD lr = new LinearRegressionWithSGD();

        lr.optimizer().setNumIterations(10);

        JavaRDD<LabeledPoint> rdd = DataFrameUtil.createLabeledPointsRDD(ctx, sqlContext, labelColumn, predictorColumns, df);

        LinearRegressionModel model = lr.run(rdd.rdd());

        workflowContext.out(model);

        // pass the computed model to the next node if it is a scoring node
        Node nextNode = this.getNode(0);
        if (nextNode != null)
        {
            if (nextNode instanceof NodeModelScore)
            {
                NodeModelScore score = (NodeModelScore)nextNode;
                score.glm = model;
                score.labelColumn = this.labelColumn;
                score.predictorColumns = this.predictorColumns;
            }
        }
    }

    //--------------------------------------------------------------------------------------

    private void ridgeRegressionWithSGD(JavaSparkContext ctx, SQLContext sqlContext, DataFrame df) {

        RidgeRegressionWithSGD lr = new RidgeRegressionWithSGD();

        lr.optimizer().setNumIterations(10);

        JavaRDD<LabeledPoint> rdd = DataFrameUtil.createLabeledPointsRDD(ctx, sqlContext, "label", "point", df);

        RidgeRegressionModel model = lr.run(rdd.rdd());

        System.out.println(model.toString());

        // pass the computed model to the next node if it is a scoring node
        Node nextNode = this.getNode(0);
        if (nextNode != null)
        {
            if (nextNode instanceof NodeModelScore)
            {
                NodeModelScore score = (NodeModelScore)nextNode;
                //score.model = model;
                //score.labelColumn = this.labelColumn;
                //score.predictorColumns = this.predictorColumns;
            }
        }
    }

    //--------------------------------------------------------------------------------------

    //--------------------------------------------------------------------------------------

    private void lassoWithSGD(JavaSparkContext ctx, SQLContext sqlContext, DataFrame df) {

        LassoWithSGD lr = new LassoWithSGD();

        lr.optimizer().setNumIterations(10);

        JavaRDD<LabeledPoint> rdd = DataFrameUtil.createLabeledPointsRDD(ctx, sqlContext, "label", "point", df);

        LassoModel model = lr.run(rdd.rdd());

        System.out.println(model.toString());

        // pass the computed model to the next node if it is a scoring node
        Node nextNode = this.getNode(0);
        if (nextNode != null)
        {
            if (nextNode instanceof NodeModelScore)
            {
                NodeModelScore score = (NodeModelScore)nextNode;
                //score.model = model;
                //score.labelColumn = this.labelColumn;
                //score.predictorColumns = this.predictorColumns;
            }
        }
    }

    //--------------------------------------------------------------------------------------

}
