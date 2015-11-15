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
import fire.nodes.dataset.NodeDataset;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jayantshekhar
 *
 * Splits a dataset into two based on the input fraction
 */
public class NodeDatasetSplit extends NodeDataset {

    public double fraction1 = 0.5;

    public NodeDatasetSplit(int i, String nm) {
        super(i, nm);

    }

    public NodeDatasetSplit()
    {

    }

    //------------------------------------------------------------------------------------------------------

    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {
        workflowContext.out("Executing NodeDatasetSplit : "+id);

        workflowContext.outSchema(df);

        DataFrame training = df.sample(true, fraction1);

        DataFrame test = df.sample(true, 1.0-fraction1);

        super.execute(ctx, sqlContext, workflowContext, training, test);

    }

    //------------------------------------------------------------------------------------------------------

}
