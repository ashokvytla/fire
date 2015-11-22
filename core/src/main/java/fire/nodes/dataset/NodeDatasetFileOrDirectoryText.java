package fire.nodes.dataset;

import fire.util.spark.SchemaUtil;
import fire.workflowengine.FireSchema;
import fire.workflowengine.WorkflowContext;
import org.apache.avro.Schema;
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

    public String colName = "text";// name of the column into which the text is loaded

    public NodeDatasetFileOrDirectoryText(int i, String nm, String p) {
        super(i, nm, p);
    }


    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {

        workflowContext.out("Executing NodeDatasetFileOrDirectoryText : "+id);

        // Load a text file
        JavaRDD<String> rdd = ctx.textFile(path);

        // Convert records of the RDD (people) to Rows.
        JavaRDD<Row> rowRDD = rdd.map(
                new Function<String, Row>() {
                    public Row call(String record) throws Exception {
                        return RowFactory.create(record);
                    }
                });

        // create a schema for the column name and Type of STRING
        StructType schema = SchemaUtil.getSchema(colName, "string");

        // Apply the schema to the RDD.
        DataFrame tdf = sqlContext.createDataFrame(rowRDD, schema);

        workflowContext.outSchema(tdf);

        super.execute(ctx, sqlContext, workflowContext, tdf);
    }

    //------------------------------------------------------------------------------------------------------

}
