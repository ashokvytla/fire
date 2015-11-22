/**
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

package fire.misc.serialization;

import fire.workflowengine.WorkflowContext;
import fire.nodes.dataset.NodeDatasetFileOrDirectoryCSV;
import fire.workflowengine.Workflow;
import com.google.gson.Gson;
import fire.workflowengine.FireSchema;
import fire.util.spark.CreateSparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jayantshekhar
 */
public class GsonSerialization {

    public static String tojson(Workflow wf) {
        Gson gson = new Gson();
        String str = gson.toJson(wf);

        return str;
    }

    //--------------------------------------------------------------------------------------

    public static Workflow fromjson(String json) {
        Gson gson = new Gson();
        Workflow wf = gson.fromJson(json, Workflow.class);

        return wf;
    }

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark and sql context
        JavaSparkContext ctx = CreateSparkContext.create(args);

        SQLContext sqlContext = new SQLContext(ctx);

        WorkflowContext workflowContext = new WorkflowContext();

        Workflow wf = createWorkflow(ctx, sqlContext);

        String json = tojson(wf);
        System.out.println("\n"+json+"\n");

        wf = fromjson(json);

        json = tojson(wf);
        System.out.println("\n"+json+"\n");

        // execute the workflow
        wf.execute(ctx, sqlContext, workflowContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static Workflow createWorkflow(JavaSparkContext ctx, SQLContext sqlContext) {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetFileOrDirectoryCSV csv1 = new NodeDatasetFileOrDirectoryCSV(1, "csv1 node", "data/cars.csv",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");
        wf.addNodeDataset(csv1);

        // test schema
        FireSchema schema = wf.getOutputSchema(1);
        if (schema != null)
            System.out.println(schema.toString());

        // split node
        //Node split = new NodeDatasetSplit(7, "split node");
        //csv1.nextNode1 = split;



        return wf;
    }

}
