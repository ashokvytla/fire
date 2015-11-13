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
import fire.ml.LabeledDocument;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

/**
 * Created by jayantshekhar
 */
public class NodeLogisticRegression extends NodeDataset implements Serializable {

    public String labelColumn = "label";
    public String predictorColumns = "f1 f2";
    public int maxIter = 10;
    public double regParam = 0.01;
    public NodeLogisticRegression() {}

    public NodeLogisticRegression(int i, String nm) {
        super(i, nm);
    }

    //--------------------------------------------------------------------------------------

    public NodeLogisticRegression(int i, String nm, String lcol, String pcols) {
        super(i, nm);

        labelColumn = lcol;
        predictorColumns = pcols;
    }

    //--------------------------------------------------------------------------------------

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {
        workflowContext.out("Executing NodeLogisticRegression : "+id);

        DataFrame lpdf = DataFrameUtil.createLabeledPointsDataFrame(ctx, sqlContext, this.labelColumn, this.predictorColumns, df);

        // print the new dataframe
        workflowContext.outSchema(lpdf);

        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(maxIter)
                .setRegParam(regParam);

        LogisticRegressionModel model = lr.fit(lpdf);

        workflowContext.out(model);

        // pass the computed model to the next node if it is a scoring node
        Node nextNode = this.getNode(0);
        if (nextNode != null)
        {
            if (nextNode instanceof NodeModelScore)
            {
                NodeModelScore score = (NodeModelScore)nextNode;
                score.model = model;
                score.labelColumn = this.labelColumn;
                score.predictorColumns = this.predictorColumns;
            }
        }

        super.execute(ctx, sqlContext, workflowContext, df);
    }

    //--------------------------------------------------------------------------------------

    // implementation using Pipeline. It is not being currently used

    public void execute111(JavaSparkContext ctx, SQLContext sqlContext, DataFrame df) {
        System.out.println("Executing NodeLogisticRegression : "+id);

        df.printSchema();

        // convert dataframe to dataframe of labeled documents
        JavaRDD<LabeledDocument> rdd = df.toJavaRDD().map(new Function<Row, LabeledDocument>() {
            public LabeledDocument call(Row row) {
                // LabeledDocument ld = new LabeledDocument(1, row.getString(0), Double.parseDouble(row.getString(1)));
                String string = row.getString(1);
                //String sss = row.getString(0);
                Double d = row.getDouble(0);

                LabeledDocument ld = new LabeledDocument(1, string, d);

                return ld;
            }
        });

        DataFrame lddf = sqlContext.createDataFrame(rdd, LabeledDocument.class);

        // print the new dataframe
        lddf.printSchema();
        lddf.show();

        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("words");

        HashingTF hashingTF = new HashingTF()
                .setNumFeatures(1000)
                .setInputCol("words")
                .setOutputCol("features");

        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.01);

        //lr.fit(df);

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {tokenizer, hashingTF, lr});

        PipelineStage[] stages = pipeline.getStages();
        for (PipelineStage stg : stages) {
            System.out.println(stg.toString());
        }

        PipelineModel model = pipeline.fit(lddf);

        System.out.println(model.fittingParamMap());
    }

    //--------------------------------------------------------------------------------------

}
