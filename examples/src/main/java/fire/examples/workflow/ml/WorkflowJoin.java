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
import fire.nodes.etl.NodeJoin;
import fire.nodes.ml.NodePrintFirstNRows;
import fire.nodes.ml.NodeSummaryStatistics;
import fire.util.spark.CreateSparkContext;
import fire.nodes.dataset.NodeDatasetFileOrDirectoryCSV;

import fire.workflowengine.Node;
import fire.workflowengine.Workflow;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowJoin {


    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark and sql context
        JavaSparkContext ctx = CreateSparkContext.create(args);

        SQLContext sqlContext = new SQLContext(ctx);

        WorkflowContext workflowContext = new WorkflowContext();

        joinwf(ctx, sqlContext, workflowContext);

        // stop the context
        ctx.stop();
    }


    //--------------------------------------------------------------------------------------

    // join workflow
    private static void joinwf(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext) {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetFileOrDirectoryCSV csv1 = new NodeDatasetFileOrDirectoryCSV(1, "csv1 node", "data/cars.csv",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");
        wf.addNodeDataset(csv1);

        // csv2 node
        NodeDatasetFileOrDirectoryCSV csv2 = new NodeDatasetFileOrDirectoryCSV(2, "csv2 node", "data/cars.csv",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");
        wf.addNodeDataset(csv2);

        // join node
        NodeJoin join = new NodeJoin(3, "join node");
        join.joinCol = "id";
        csv1.addNode(join);
        csv2.addNode(join);

        // print node
        Node print = new NodePrintFirstNRows(4, "print node", 10);
        join.addNode(print);

        // summary statistics node
        Node summaryStatistics = new NodeSummaryStatistics(5, "summary statistics");
        print.addNode(summaryStatistics);

        wf.execute(ctx, sqlContext, workflowContext);
    }

}
