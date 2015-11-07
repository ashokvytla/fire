package fire.nodes.examples.ml;

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

import fire.workflowengine.WorkflowContext;
import fire.workflowengine.NodeSchema;
import fire.nodes.ml.NodeLinearRegressionWithSGD;
import fire.sparkutil.CreateSparkContext;
import fire.nodes.dataset.NodeDatasetFileOrDirectoryCSV;

import fire.workflowengine.Workflow;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowHousing {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark and sql context
        JavaSparkContext ctx = CreateSparkContext.create(args);

        SQLContext sqlContext = new SQLContext(ctx);

        WorkflowContext workflowContext = new WorkflowContext();

        executewfHousingLinearRegression(ctx, sqlContext, workflowContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void executewfHousingLinearRegression(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext) {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetFileOrDirectoryCSV csv1 = new NodeDatasetFileOrDirectoryCSV(1, "csv1 node", "data/housing.csv",
                "id price lotsize bedrooms bathrms stories driveway recroom fullbase gashw airco garagepl prefarea",
                "string double double double double double string string string string string double string",
                "numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric");

        csv1.filterLinesContaining = "price";

        wf.addNodeDataset(csv1);

        // test schema
        NodeSchema schema = wf.getSchema(1);
        if (schema != null)
            System.out.println(schema.toString());

        // linear regression node
        NodeLinearRegressionWithSGD nodeLinearRegressionWithSGD =
                        new NodeLinearRegressionWithSGD(10, "NodeLinearRegressionWithSGD node", "price", "lotsize bedrooms");

        csv1.addNode(nodeLinearRegressionWithSGD);

        wf.execute(ctx, sqlContext, workflowContext);
    }

}
