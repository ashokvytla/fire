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

package fire.nodes.dataset;

import fire.workflowengine.Node;
import fire.workflowengine.NodeSchema;
import org.apache.spark.sql.types.StructType;

/**
 * Created by jayantshekhar
 * Represents a Node that points to a Dataset - a Dataset could be data on hdfs.
 */
public abstract class NodeDataset extends Node {

    // The schema is encoded in a string
    public String columns = "label text f";
    public String columnTypes   = "double string double";
    public String columnmlTypes   = "numeric text numeric";

    //--------------------------------------------------------------------------------------

    public NodeDataset(int i, String nm) {
        super(i, nm);
    }

    //--------------------------------------------------------------------------------------

    public NodeDataset(int i, String nm, String cols, String colTypes) {
        super(i, nm);

        columns = cols;
        columnTypes = colTypes;
    }

    //--------------------------------------------------------------------------------------

    public NodeDataset()
    {}

    //--------------------------------------------------------------------------------------

    // get the schema of this node
    public NodeSchema getOutputSchema() {
        return new NodeSchema(columns, columnTypes, columnmlTypes);
    }

    //--------------------------------------------------------------------------------------

    @Override
    public NodeSchema getOutputSchema(int nodeId, NodeSchema inputSchema) {

        // get the schema for this node dataset
        NodeSchema s = getOutputSchema();

        // if node id matches
        if (this.id == nodeId)
            return s;

        // else send it to the super class to ask the subsequent nodes
        return super.getOutputSchema(nodeId, s);
    }

    //--------------------------------------------------------------------------------------

    // get the spark sql schema
    public StructType getSparkSQLSchema() {
        final StructType schema = new NodeSchema(columns, columnTypes, columnmlTypes).getSparkSQLStructType();

        return schema;
    }

    //--------------------------------------------------------------------------------------

}
