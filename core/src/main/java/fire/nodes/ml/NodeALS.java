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

import fire.workflowengine.NodeDataset;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

/**
 * Created by jayantshekhar
 */

// case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
public class NodeALS extends NodeDataset implements Serializable {

    public String userCol;
    public String itemCol;
    public String ratingCol;

    public NodeALS() {}

    public NodeALS(int i, String nm) {
        super(i, nm);
    }

    public NodeALS(int i, String nm, String ucol, String icol, String rcol) {
        super(i, nm);

        userCol = ucol;
        itemCol = icol;
        ratingCol = rcol;
    }

    //------------------------------------------------------------------------------------------------------

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {
        workflowContext.out("Executing NodeALS : "+id);

        df.printSchema();

        ALS als = new ALS().setUserCol(userCol).setItemCol(itemCol).setRatingCol(ratingCol);

        ALSModel model = als.fit(df);

        DataFrame newdf = model.transform(df);

        workflowContext.outSchema(newdf);
    }

    //------------------------------------------------------------------------------------------------------

}
