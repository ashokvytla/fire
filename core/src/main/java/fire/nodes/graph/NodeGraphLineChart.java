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

package fire.nodes.graph;

import fire.nodes.graph.NodeGraph;
import fire.workflowengine.WorkflowContext;
import fire.dataframeutil.DataFrameUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.collection.Seq;

import java.io.Serializable;

/**
 * Created by jayantshekhar
 */

// http://www.harding.edu/fmccown/r/

public class NodeGraphLineChart extends NodeGraph implements Serializable {

    public final static int LINE_CHART = 0;
    public final static int SIDE_BY_SIDE_BAR_CHART = 1; // this is the default bar chart
    public final static int STACKED_BAR_CHAR = 2;
    public final static int PIE_CHAR = 3;

    public int graphType = LINE_CHART;

    // columns to display in the line chart
    public String columns = "label f1";

    public NodeGraphLineChart(int i, String nm) {
        super(i, nm);
    }

    public NodeGraphLineChart()
    {

    }

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {

        workflowContext.out("Executing NodeGraphLineChart : " + id);

        Seq<Column> seq = DataFrameUtil.getColumnsAsSeq(df, columns);

        // select the required columns from the input dataframe
        DataFrame selectdf = df.select(seq);

        workflowContext.outSchema(selectdf);

        super.execute(ctx, sqlContext, workflowContext, df);

    }

}
