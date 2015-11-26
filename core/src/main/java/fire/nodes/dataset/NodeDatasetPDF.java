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

import fire.util.fileformats.pdf.PdfInputFormat;
import fire.util.spark.SchemaUtil;
import fire.workflowengine.WorkflowContext;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jayantshekhar
 * Represents a Dataset Node which points to data in a File or Directory with data in csv format.
 */

public class NodeDatasetPDF extends NodeDatasetFileOrDirectory implements Serializable {

    // key column
    public String kcol = "key";

    // value column
    public String vcol = "val";

    public NodeDatasetPDF(int i, String nm, String p) {
        super(i, nm, p);
    }

    public NodeDatasetPDF(int i, String nm, String p, String cols, String colTypes, String colmlTypes) {
        super(i, nm, p, cols, colTypes);
    }

    public NodeDatasetPDF()
    {

    }

    //------------------------------------------------------------------------------------------------------

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {

        workflowContext.out("Executing NodeDatasetPDF : " + id);

        JavaPairRDD<Text, Text> pairRDD = ctx.newAPIHadoopFile(path, PdfInputFormat.class,
                    Text.class, Text.class, ctx.hadoopConfiguration());

        // Convert records of the RDD to Rows.
        JavaRDD<Row> rowRDD = pairRDD.map(
                new Function<Tuple2<Text, Text>, Row>() {
                    public Row call(Tuple2<Text, Text> t2) throws Exception {
                        return RowFactory.create(t2._1().toString(), t2._2().toString());
                    }
                });

        // create a schema for the column name and Type of STRING
        StructType schema = SchemaUtil.getSchema(kcol + " " + vcol, "string string");

        // Apply the schema to the RDD.
        DataFrame tdf = sqlContext.createDataFrame(rowRDD, schema);

        workflowContext.outSchema(tdf);

        super.execute(ctx, sqlContext, workflowContext, tdf);
    }

    //------------------------------------------------------------------------------------------------------

}
