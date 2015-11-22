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

package fire.workflowengine;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jayantshekhar
 * Represents any node in the workflow.
 */
public abstract class Node {

    // node id
    public int id;

    // node name
    public String name;

    // nodes list
    public List<Node> nextNodes = new LinkedList<>();

    //--------------------------------------------------------------------------------------

    public Node()
    {

    }

    //--------------------------------------------------------------------------------------

    public Node getNode(int idx)
    {
        if (nextNodes.size() >= (idx + 1))
            return nextNodes.get(idx);

        return null;
    }

    public void addNode(Node node) {
        nextNodes.add(node);
    }

    //--------------------------------------------------------------------------------------

    public Node(int i, String nm) {
        id = i;
        name = nm;
    }

    //--------------------------------------------------------------------------------------

    // check if there is a circular path in the workflow
    public boolean isTraversalCircular(Integer numNodesVisited) {
        numNodesVisited++;

        if (numNodesVisited > 500)
            return true;

        Iterator<Node> iterator = nextNodes.iterator();
        while (iterator.hasNext()) {
            Node nextNode = iterator.next();
            boolean result = nextNode.isTraversalCircular(numNodesVisited);
            if (result)
                return true;
        }

        return false;
    }

    //--------------------------------------------------------------------------------------

    /***
    public boolean isCircular(HashMap<Integer, Node> hm) {

        if (hm.get(id) != null) {
            return true;
        }

        hm.put(id, this);

        Iterator<Node> iterator = nextNodes.iterator();
        while (iterator.hasNext()) {
            Node nextNode = iterator.next();
            boolean result = nextNode.isCircular(hm);
            if (result)
                return true;
        }

        return false;
    }
     ***/

    //--------------------------------------------------------------------------------------

    // get the schema of a given node given the schema for this node
    public NodeSchema getOutputSchema(int nodeId, NodeSchema inputSchema) {

        // return the incoming schema if the node id matches. nodes can override this behavior by implementing getSchema
        if (nodeId == this.id)
            return inputSchema;

        Iterator<Node> iterator = nextNodes.iterator();
        while (iterator.hasNext()) {
            Node nextNode = iterator.next();
            NodeSchema schema = nextNode.getOutputSchema(nodeId, inputSchema);
            if (schema != null)
                return schema;
        }

        return null;
    }

    //--------------------------------------------------------------------------------------

    // execute the next nodes given the ougoing dataframe of this node
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {
        System.out.println("Executing node : "+id);

        Iterator<Node> iterator = nextNodes.iterator();
        while (iterator.hasNext()) {
            Node nextNode = iterator.next();
            nextNode.execute(ctx, sqlContext, workflowContext, df);
        }

    }

    //--------------------------------------------------------------------------------------

    // execute the next nodes given the ougoing dataframes of this node
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df1, DataFrame df2) {
        System.out.println("Executing node : "+id);

        Node nextNode1 = this.getNode(0);

        if (nextNode1 != null)
        {
            nextNode1.execute(ctx, sqlContext, workflowContext, df1);
        }

        Node nextNode2 = this.getNode(1);
        if (nextNode2 != null)
        {
            nextNode2.execute(ctx, sqlContext, workflowContext, df2);
        }
    }

    //--------------------------------------------------------------------------------------

}
