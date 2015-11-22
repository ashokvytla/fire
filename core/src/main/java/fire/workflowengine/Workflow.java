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

package fire.workflowengine;

import fire.nodes.dataset.NodeDataset;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;

/**
 * Created by jayantshekhar
 */
public class Workflow {

    // the starting dataset nodes in the workflow. each subsequent node points to it next nodes
    public ArrayList<NodeDataset> datasetNodes = new ArrayList<>();

    //--------------------------------------------------------------------------------------

    // get the schema for a given node id
    public FireSchema getOutputSchema(int nodeId) {
        for (NodeDataset nodeDataset : datasetNodes) {
            FireSchema schema = nodeDataset.getOutputSchema(nodeId, null);
            if (schema != null)
                return schema;
        }

        return null;
    }

    //--------------------------------------------------------------------------------------

    // check if there is any circular traversal in the workflow
    // tries to see if it runs into > 500 nodes when traversing. we assume that we would not have more than
    // 500 nodes in any workflow
    // WE ARE CURRENTLY USING THIS ONE AS AGAINST isCircular()
    public boolean isTraversalCircular() {

        Integer numNodesVisited = 0;

        for (NodeDataset nodeDataset : datasetNodes) {
            boolean result = nodeDataset.isTraversalCircular(numNodesVisited);
            if (result)
                return true;
        }

        return false;
    }

    //--------------------------------------------------------------------------------------

    /***
    // check if there is any circular traversal in the workflow
    // it uses a hashmap to track the nodes it has visited
    public boolean isCircular() {

        HashMap<Integer, Node> hashMap = new HashMap<Integer, Node>();

        for (NodeDataset nodeDataset : datasetNodes) {
            boolean result = nodeDataset.isCircular(hashMap);
            if (result)
                return true;
        }

        return false;
    }
     ***/

    //--------------------------------------------------------------------------------------

    // execute the workflow
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext) {
        for (NodeDataset nodeDataset : datasetNodes) {
            nodeDataset.execute(ctx, sqlContext, workflowContext, null);
        }

    }

    //--------------------------------------------------------------------------------------

    // add dataset node to the workflow
    public void addNodeDataset(NodeDataset node) {
        datasetNodes.add(node);
    }

    //--------------------------------------------------------------------------------------

    // convert the workflow to json
    public String tojson() {

        String str = Serializer.tojson(this);

        return str;
    }

    //--------------------------------------------------------------------------------------

    // create the workflow from a json string
    public static Workflow fromjson(String json) {

        Workflow wf = Serializer.fromjson(json);

        return wf;
    }

    //--------------------------------------------------------------------------------------

}
