package fire.util.spark;

import fire.workflowengine.FireSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jayantshekhar on 11/21/15.
 */
public class SchemaUtil {

    public static StructType getSchema(String colnames, String coltypes, String colmltypes) {
        StructType schema = new FireSchema(colnames, coltypes, colmltypes).getSparkSQLStructType();
        return schema;
    }

    /***
    // get the schema given the column names and column types
    public static StructType getSparkSQLStructType111(String columns, org.apache.avro.Schema.Type[] columnTypes) {

        String[] columnNames = columns.split(" ");
        List<StructField> fields = new ArrayList<StructField>();

        int idx = 0;
        for (String fieldName: columnNames) {
            DataType dataType = null;

            if (columnTypes[idx] == org.apache.avro.Schema.Type.INT)
            {
                dataType = DataTypes.IntegerType;
            } else if (columnTypes[idx] == org.apache.avro.Schema.Type.DOUBLE)
            {
                dataType = DataTypes.DoubleType;
            } else if (columnTypes[idx] == org.apache.avro.Schema.Type.STRING)
            {
                dataType = DataTypes.StringType;
            }

            fields.add(DataTypes.createStructField(fieldName, dataType, true)); // name, datatype, nullable

            idx++;
        }

        StructType structType = DataTypes.createStructType(fields);

        return structType;
    }
     ***/


}
