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
import fire.workflowengine.Node;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

/**
 * Created by jayantshekhar
 */
public class NodePrintFirstNRows extends Node implements Serializable {

    public int n = 5;

    public NodePrintFirstNRows(int i, String nm, int tempn) {
        super(i, nm);

        n = tempn;
    }

    public NodePrintFirstNRows()
    {

    }

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {

        workflowContext.out("Executing NodePrintFirstNRows : "+id);

        Row[] rows = df.take(n);

        for (Row row : rows) {
            workflowContext.out(row.toString());
        }

        super.execute(ctx, sqlContext, workflowContext, df);
    }

}
