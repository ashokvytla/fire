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

package fire.examples.workflow.ml;

import fire.workflowengine.WorkflowContext;
import fire.workflowengine.NodeSchema;
import fire.nodes.ml.NodeDatasetSplit;
import fire.nodes.ml.NodeLinearRegression;
import fire.nodes.ml.NodeModelScore;
import fire.nodes.ml.NodeStandardScaler;
import fire.sparkutil.CreateSparkContext;
import fire.nodes.dataset.NodeDatasetFileOrDirectoryCSV;

import fire.workflowengine.Node;
import fire.workflowengine.Workflow;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowLinearRegression {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark and sql context
        JavaSparkContext ctx = CreateSparkContext.create(args);

        SQLContext sqlContext = new SQLContext(ctx);

        WorkflowContext workflowContext = new WorkflowContext();

        lrwf(ctx, sqlContext, workflowContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void lrwf(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext) {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetFileOrDirectoryCSV csv1 = new NodeDatasetFileOrDirectoryCSV(1, "csv1 node", "data/cars.csv",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");
        wf.addNodeDataset(csv1);

        // test schema
        NodeSchema schema = wf.getSchema(1);
        if (schema != null)
            System.out.println(schema.toString());

        // split node
        Node split = new NodeDatasetSplit(7, "split node");
        csv1.addNode(split);

        // standard scaler node
        NodeStandardScaler standardScaler = new NodeStandardScaler(10, "standard Scaler node");
        split.addNode(standardScaler);

        // linear regression node
        NodeLinearRegression regression = new NodeLinearRegression(8, "linear regression node", "label", "f1 f2");
        regression.maxIter = 10;
        regression.regParam = .01;
        split.addNode(regression);

        // score model node
        Node score = new NodeModelScore(9, "score node");
        split.addNode(score);
        regression.addNode(score);

        // execute the workflow
        wf.execute(ctx, sqlContext, workflowContext);

    }
}
