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

package fire.examples.workflow.etl;

import fire.nodes.dataset.*;
import fire.nodes.ml.NodePrintFirstNRows;
import fire.workflowengine.WorkflowContext;
import fire.nodes.ml.NodeHashingTF;
import fire.nodes.ml.NodeTokenizer;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.Workflow;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowPdfImagesOCR {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark and sql context
        JavaSparkContext ctx = CreateSparkContext.create(args);

        SQLContext sqlContext = new SQLContext(ctx);

        WorkflowContext workflowContext = new WorkflowContext();

        pdfwf(ctx, sqlContext, workflowContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void pdfwf(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext) {

        Workflow wf = new Workflow();

        // pdf node
        NodeDatasetPDFImageOCR pdf = new NodeDatasetPDFImageOCR(1, "pdf node", "data/scansmpl.pdf");
        wf.addNodeDataset(pdf);

        // print first 1 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(2, "print first 1 row", 1);
        pdf.addNode(nodePrintFirstNRows);

        wf.execute(ctx, sqlContext, workflowContext);
    }

}
