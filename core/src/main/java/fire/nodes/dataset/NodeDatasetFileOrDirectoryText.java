package fire.nodes.dataset;

import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.List;

/**
 * Created by jayantshekhar on 11/12/15.
 */
public class NodeDatasetFileOrDirectoryText extends NodeDatasetFileOrDirectory implements Serializable {

    public NodeDatasetFileOrDirectoryText(int i, String nm, String p) {
        super(i, nm, p);
    }


    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {

        workflowContext.out("Executing NodeDatasetFileOrDirectoryText : "+id);

        // Load a text file and convert each line to a JavaBean.
        JavaRDD<String> rdd = ctx.textFile(path);

        String schemaString = "col";

        List<StructField> fields = new java.util.ArrayList<StructField>();
        for (String fieldName: schemaString.split(" ")) {
            fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
        }
        StructType schema = DataTypes.createStructType(fields);


        // Convert records of the RDD (people) to Rows.
        JavaRDD<Row> rowRDD = rdd.map(
                new Function<String, Row>() {
                    public Row call(String record) throws Exception {
                        return RowFactory.create(record);
                    }
                });

        // Apply the schema to the RDD.
        DataFrame peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema);

        // Apply the schema to the RDD.
        // It is important to make sure that the structure of every [[Row]] of the provided RDD matches
        // the provided schema. Otherwise, there will be runtime exception.
        DataFrame tdf = sqlContext.createDataFrame(rowRDD, schema);

        super.execute(ctx, sqlContext, workflowContext, tdf);
    }

    //------------------------------------------------------------------------------------------------------

}
