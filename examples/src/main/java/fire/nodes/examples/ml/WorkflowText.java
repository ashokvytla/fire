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

package fire.nodes.examples.ml;

import fire.workflowengine.WorkflowContext;
import fire.workflowengine.NodeSchema;
import fire.nodes.ml.NodeHashingTF;
import fire.nodes.ml.NodeTokenizer;
import fire.sparkutil.CreateSparkContext;
import fire.nodes.dataset.NodeDatasetFileOrDirectoryCSV;
import fire.workflowengine.Workflow;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowText {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark and sql context
        JavaSparkContext ctx = CreateSparkContext.create(args);

        SQLContext sqlContext = new SQLContext(ctx);

        WorkflowContext workflowContext = new WorkflowContext();

        textwf(ctx, sqlContext, workflowContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void textwf(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext) {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetFileOrDirectoryCSV csv1 = new NodeDatasetFileOrDirectoryCSV(1, "csv1 node", "data/spam1.csv",
                "docid doc label", "int string double",
                "numeric numeric numeric");
        wf.addNodeDataset(csv1);

        // test schema
        NodeSchema schema = wf.getSchema(1);
        if (schema != null)
            System.out.println(schema.toString());

        // tokenizer node
        NodeTokenizer tokenizer = new NodeTokenizer(2, "tokenizer node");
        csv1.addNode(tokenizer);

        // hashing TF node
        NodeHashingTF hashingtf = new NodeHashingTF(2, "hashing TF node");
        tokenizer.addNode(hashingtf);

        wf.execute(ctx, sqlContext, workflowContext);
    }

}
