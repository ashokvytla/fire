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
import fire.noderesult.NodeResultGraph;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.io.Serializable;

/**
 * Created by jayantshekhar
 */
public class NodeGraphHistogram extends NodeGraph implements Serializable {

    public String groupByColumn = "f1";

    public NodeGraphHistogram(int i, String nm) {
        super(i, nm);
    }

    public NodeGraphHistogram()
    {

    }

    //------------------------------------------------------------------------------------------------------

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {

        System.out.println("Executing NodeGraphHistogram : "+id);

        workflowContext.outSchema(df);

        // find counts of groups
        GroupedData grouped = df.groupBy(groupByColumn);
        DataFrame groupedCount = grouped.count();

        workflowContext.outSchema(groupedCount);
        groupedCount.show();

        long count = groupedCount.count();
        int x[] = new int[(int)count];
        for (int i=0; i<x.length; i++)
            x[i] = i;

        double y[] = new double[(int)count];
        Row[] rows = groupedCount.collect();
        for (int i=0; i<y.length; i++)
            y[i] = rows[i].getDouble(0);


        // create results object
        NodeResultGraph resultGraph = new NodeResultGraph(this.id, "Histogram Graph", x, xlabel, y, ylabel);

        workflowContext.outResult(resultGraph);


        super.execute(ctx, sqlContext, workflowContext, df);
    }

    //------------------------------------------------------------------------------------------------------

}
