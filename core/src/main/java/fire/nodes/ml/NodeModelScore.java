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
import fire.workflowengine.NodeDataset;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Model;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.regression.GeneralizedLinearModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

/**
 * Created by jayantshekhar
 */
public class NodeModelScore extends NodeDataset implements Serializable {

    // these get passed to this node by the previous modeling step. these are also transient because we do not want
    // them to be part of the serialization and json process
    public transient String labelColumn = "label";
    public transient String predictorColumns = "f1 f2";
    public transient Model model = null;

    public transient GeneralizedLinearModel glm = null;

    public NodeModelScore() {}

    public NodeModelScore(int i, String nm) {
        super(i, nm);
    }

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {
        workflowContext.out("Executing NodeModelScore : " + id);

        if (model != null) {
            scoreModel(ctx, sqlContext, workflowContext, df);
            return;
        }

        if (glm != null) {
            scoreGLM(ctx, sqlContext, df);
            return;
        }

    }

    private void scoreGLM(JavaSparkContext ctx, SQLContext sqlContext, DataFrame df) {

        // create a dataframe of labeled points
        DataFrame lpdf = DataFrameUtil.createLabeledPointsDataFrame(ctx, sqlContext, labelColumn, predictorColumns, df);

        // make the predictions
        DataFrame predictions = model.transform(lpdf);

    }

    private void scoreModel(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {

        // create a dataframe of labeled points
        DataFrame lpdf = DataFrameUtil.createLabeledPointsDataFrame(ctx, sqlContext, labelColumn, predictorColumns, df);

        // make the predictions
        DataFrame predictions = model.transform(lpdf);

        // print the predictions
        predictions.printSchema();
        predictions.show();

        for (Row r: predictions.select("features", "label", "prediction").collect()) {
            workflowContext.out("(" + r.get(0) + ", " + r.get(1) + ") " +
                    ", prediction=" + r.get(2));
        }

        //-----------------------------------------------------------------

        // COMPUTE AND PRINT CONFUSION MATRIX

        DataFrame pred_label = predictions.select("prediction", "label");

        // obtain metrics
        MulticlassMetrics metrics = new MulticlassMetrics(DataFrameUtil.createPredictionLabelRDD(ctx, sqlContext, pred_label).rdd());
        Matrix confusionMatrix = metrics.confusionMatrix();

        // output the Confusion Matrix
        workflowContext.out("Confusion Matrix");
        workflowContext.out(confusionMatrix);

        super.execute(ctx, sqlContext, workflowContext, df);
    }

}
