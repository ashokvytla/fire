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

import java.util.List;

import fire.nodes.dataset.NodeDataset;
import com.google.common.collect.Lists;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

/**
 * Created by jayantshekhar
 * Represents a Dataset Node that creates LabeledPoints in the driver memory and parallalizes it.
 */
public class NodeDatasetLabeledPoint extends NodeDataset {

    public NodeDatasetLabeledPoint(int i, String nm) {
        super(i, nm);
    }

    //------------------------------------------------------------------------------------------------------

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {
        workflowContext.out("Executing NodeDatasetLabeledPoint : "+id);

        List<LabeledPoint> localTraining = Lists.newArrayList(
                new LabeledPoint(1.0, Vectors.dense(0.0, 1.1, 0.1)),
                new LabeledPoint(0.0, Vectors.dense(2.0, 1.0, -1.0)),
                new LabeledPoint(0.0, Vectors.dense(2.0, 1.3, 1.0)),
                new LabeledPoint(1.0, Vectors.dense(0.0, 1.2, -0.5)));

        DataFrame training = sqlContext.createDataFrame(ctx.parallelize(localTraining), LabeledPoint.class);

        String[] cols = training.columns();
        for (String col : cols) {
            System.out.println(col);
        }

        StructType structType = training.schema();
        System.out.println(structType.toString());

        Tuple2<String, String> dtypes[] = training.dtypes();

        for (Tuple2<String, String> col : dtypes) {
            System.out.println(col._1()+" "+col._2().toString());
        }

        training.printSchema();

        training.show();

        Row row = training.first();

        System.out.println(row.toString());


    }

    //------------------------------------------------------------------------------------------------------

}
