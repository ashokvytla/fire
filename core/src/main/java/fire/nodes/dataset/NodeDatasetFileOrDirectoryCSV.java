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

import fire.workflowengine.WorkflowContext;
import fire.workflowengine.NodeDatasetFileOrDirectory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jayantshekhar
 * Represents a Dataset Node which points to data in a File or Directory with data in csv format.
 */

public class NodeDatasetFileOrDirectoryCSV extends NodeDatasetFileOrDirectory implements Serializable {

    // filter the lines that contain this string. used to filter out the header lines
    public String filterLinesContaining = null;

    public NodeDatasetFileOrDirectoryCSV(int i, String nm, String p) {
        super(i, nm, p);
    }

    public NodeDatasetFileOrDirectoryCSV(int i, String nm, String p, String cols, String colTypes, String colmlTypes) {
        super(i, nm, p, cols, colTypes);
    }

    public NodeDatasetFileOrDirectoryCSV()
    {

    }

    public Object parseField(String string, StructField field) {

        Object f1;

        if (field.dataType().sameType(DataTypes.IntegerType)) {
            f1 = Integer.parseInt(string); return f1;
        }
        if (field.dataType().sameType(DataTypes.DoubleType)) {
            f1 = Double.parseDouble(string); return f1;
        }
        if (field.dataType().sameType(DataTypes.StringType)) {
            return string.trim();
        }

        return string;
    }

    //------------------------------------------------------------------------------------------------------

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {

        System.out.println("Executing NodeDatasetFileOrDirectoryCSV : "+id);

        // Load a text file and convert each line to a JavaBean.
        JavaRDD<String> people = ctx.textFile(path);

        // filter the header row
        if (filterLinesContaining != null) {
            people = people.filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String s) throws Exception {
                    if (s.contains(filterLinesContaining))
                        return false;

                    return true;
                }
            });
        }

        // get schema
        final StructType schema = getSparkSQLSchema();

        // Convert records of the RDD (people) to Rows.
        JavaRDD<Row> rowRDD = people.flatMap(
                new FlatMapFunction<String, Row>() {

                    @Override
                    public Iterable<Row> call(String record) throws Exception {
                        List<Row> ll = new LinkedList<Row>();

                        String[] fields = record.split(",");

                        // skip invalid records
                        if (fields.length != schema.length())
                            return ll;

                        Object f[] = new Object[fields.length];
                        int idx = 0;
                        for (String field : fields) {
                            f[idx] = parseField(fields[idx], schema.fields()[idx]);
                            idx++;
                        }

                        Row row = RowFactory.create(f);
                        ll.add(row);

                        return ll;
                    }
                });

        // Apply the schema to the RDD.
        // It is important to make sure that the structure of every [[Row]] of the provided RDD matches
        // the provided schema. Otherwise, there will be runtime exception.
        DataFrame tdf = sqlContext.createDataFrame(rowRDD, schema);

        tdf.printSchema();
        tdf.show();

        super.execute(ctx, sqlContext, workflowContext, tdf);
    }

    //------------------------------------------------------------------------------------------------------

}
