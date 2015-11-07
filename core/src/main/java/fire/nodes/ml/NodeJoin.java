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

import fire.workflowengine.WorkflowContext;
import fire.workflowengine.NodeSchema;
import fire.workflowengine.Node;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

/**
 * Created by jayantshekhar
 */
public class NodeJoin extends Node implements Serializable {

    public String joinCol = "id";

    public transient DataFrame dataFrame = null;
    public transient NodeSchema schema = null;

    public NodeJoin(int i, String nm) {
        super(i, nm);

    }

    public NodeJoin()
    {

    }


    @Override
    public NodeSchema getSchema(int nodeId, NodeSchema sch) {

        // save the incoming schema and wait for the next invocation. do not also call getSchema on the outgoing edge now
        if (schema == null)
        {
            schema = sch;
            return null;
        }

        NodeSchema joinSchema = schema.join(sch, joinCol);

        if (this.id == nodeId) {
            return joinSchema;
        }

        return super.getSchema(nodeId, joinSchema);
    }

    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {

        System.out.println("Executing NodeJoin : "+id);

        // if this is the first dataframe
        if (dataFrame == null)
        {
            dataFrame = df;
            return;
        }

        // print schema
        dataFrame.printSchema();
        df.printSchema();

        // if this is the second dataframe
        DataFrame joindf = dataFrame.join(df, df.col(joinCol).equalTo(dataFrame.col(joinCol)));

        joindf.printSchema();
        joindf.show();

        super.execute(ctx, sqlContext, workflowContext, joindf);
    }

}
