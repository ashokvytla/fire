package fire.util.spark;

import fire.workflowengine.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.mutable.ListBuffer;

/**
 * Created by jayantshekhar
 */
public class DataFrameUtil {

    // get the indexes of the columns in the given dataframe
    public static int[] getColumnIndexes(DataFrame df, String colstr) {

        // columns in the dataframe
        String[] columns = df.columns();

        // find the indexes of the data frame columns
        final String[] dcols = colstr.split(" ");
        final int[] dcolsidx = new int[dcols.length];

        for (int i=0; i<dcols.length; i++) {
            for (int j=0; j<columns.length; j++) {
                if (dcols[i].equals(columns[j]))
                    dcolsidx[i] = j;
            }
        }

        return dcolsidx;
    }

    // get the index of the column in the given dataframe
    public static int getColumnIndex(DataFrame df, String col) {

        // columns in the dataframe
        String[] columns = df.columns();

        for (int i=0; i<columns.length; i++) {
            if (col.equals(columns[i]))
                return i;
        }

        return -1;
    }

    //-----------------------------------------------------------------------------------------------

    public static Seq<Column> getColumnsAsSeq(DataFrame df, String columns) {

        String[] carr = columns.split(" ");

        // create a list of relevant columns from the dataframe
        ListBuffer<Column> list = new ListBuffer<>();

        for (String s : carr) {
            Column column = df.col(s);
            list.$plus$eq(column);
        }

        return list;
    }

    //------------------------------------------------------------------------------------------------------------

    // create an RDD of predictions and labels
    public static JavaRDD<Tuple2<Object, Object>> createPredictionLabelRDD(JavaSparkContext ctx, SQLContext sqlContext, DataFrame df) {

        JavaRDD<Tuple2<Object, Object>> rdd = df.toJavaRDD().map(new Function<Row, Tuple2<Object, Object>>() {

            @Override
            public Tuple2<Object, Object> call(Row row) {

                double d1 = row.getDouble(0);
                double d2 = row.getDouble(1);

                return new Tuple2<Object, Object>(d1, d2);
            }
        });

        return rdd;
    }

    //------------------------------------------------------------------------------------------------------------

    // create an RDD of Vectors from a DataFrame
    public static JavaRDD<Vector> createVectorRDD(JavaSparkContext ctx, SQLContext sqlContext, String cols, DataFrame df) {

        // columns in the dataframe
        String[] columns = df.columns();

        // find the indexes of the columns
        final String[] pcols = cols.split(" ");
        final int[] pcolsidx = new int[pcols.length];

        for (int i=0; i<pcols.length; i++) {
            for (int j=0; j<columns.length; j++) {
                if (pcols[i].equals(columns[j]))
                    pcolsidx[i] = j;
            }
        }

        // convert dataframe to an RDD of Vectors
        JavaRDD<Vector> rdd = df.toJavaRDD().map(new Function<Row, Vector>() {

            @Override
            public Vector call(Row row) {

                double[] arr = new double[pcolsidx.length];
                for (int i=0; i<pcolsidx.length; i++) {
                    arr[i] = row.getDouble(pcolsidx[i]);
                }

                Vector v = Vectors.dense(arr);

                return v;
            }
        });

        return rdd;
    }

    //--------------------------------------------------------------------------------------------------

    // create an RDD of LabeledPoints from a DataFrame
    public static JavaRDD<LabeledPoint> createLabeledPointsRDD(JavaSparkContext ctx, SQLContext sqlContext,
                                                               String labelcol, String featurescol, DataFrame df) {

        df.printSchema();
        Seq<Column> seq = getColumnsAsSeq(df, labelcol + " " + featurescol);
        DataFrame newdf = df.select(seq);
        newdf.printSchema();

        // convert dataframe to an RDD of Vectors
        JavaRDD<LabeledPoint> rdd = newdf.toJavaRDD().map(new Function<Row, LabeledPoint>() {

            @Override
            public LabeledPoint call(Row row) {

                double[] arr = new double[row.length()-1];
                for (int i = 0; i<row.length()-1; i++) {
                    arr[i] = row.getDouble(i+1);
                }

                LabeledPoint labeledPoint = new LabeledPoint(row.getDouble(0), Vectors.dense(arr));

                return labeledPoint;
            }
        });

        return rdd;
    }

    //--------------------------------------------------------------------------------------------------

    // create a DataFrame from a Vector RDD
    public static DataFrame createDataFrame(JavaSparkContext ctx, SQLContext sqlContext, JavaRDD<Vector> rdd, String columns, String columnmlTypes) {

        String columnTypes = "";

        String columnarr[] = columns.split(" ");

        for (String s : columnarr) {
            columnTypes += "double ";
        }

        // get schema
        final StructType schema = new Schema(columns, columnTypes, columnmlTypes).getSparkSQLStructType();

        // Convert records of the RDD (people) to Rows.
        JavaRDD<Row> rowRDD = rdd.map(
                new Function<Vector, Row>() {

                    @Override
                    public Row call(Vector v) throws Exception {

                        Row row = vectorToRow(v);

                        return row;
                    }
                });

        // Apply the schema to the RDD.
        DataFrame tdf = sqlContext.createDataFrame(rowRDD, schema);

        return tdf;
    }

    //-----------------------------------------------------------------------------------------------

    // convert a Vector to a Row
    public static Row vectorToRow(Vector v) {
        Object f[] = new Object[v.size()];
        int idx = 0;

        double[] arr = v.toArray();

        Row row = RowFactory.create(arr);

        return row;
    }

    //-----------------------------------------------------------------------------------------------

    // create a DataFrame of LabeledPoints : label and Vector : from a given DataFrame
    public static DataFrame createLabeledPointsDataFrame(JavaSparkContext ctx, SQLContext sqlContext, String labelColumn, String predictorColumns, DataFrame df) {
        //Column column = df.col(labelColumn);

        // columns in the dataframe
        String[] columns = df.columns();

        // find the index of the label column
        int idxOfLabelTemp = 0;
        for (int i=0; i<columns.length; i++) {
            if (columns[i].equals(labelColumn))
            {
                idxOfLabelTemp = i;
                break;
            }
        }

        final int idxOfLabel = idxOfLabelTemp;

        // find the indexes of the predictor columns
        final String[] pcols = predictorColumns.split(" ");
        final int[] pcolsidx = new int[pcols.length];

        for (int i=0; i<pcols.length; i++) {
            for (int j=0; j<columns.length; j++) {
                if (pcols[i].equals(columns[j]))
                    pcolsidx[i] = j;
            }
        }

        // convert dataframe to dataframe of labeled points
        JavaRDD<LabeledPoint> rdd = df.toJavaRDD().map(new Function<Row, LabeledPoint>() {
            public LabeledPoint call(Row row) {

                Double label = row.getDouble(idxOfLabel);

                double[] arr = new double[pcolsidx.length];
                for (int i=0; i<pcolsidx.length; i++) {
                    arr[i] = row.getDouble(pcolsidx[i]);
                }

                Vector v = Vectors.dense(arr);

                LabeledPoint labeledPoint = new LabeledPoint(label, v);

                return labeledPoint;
            }
        });

        DataFrame lpdf = sqlContext.createDataFrame(rdd, LabeledPoint.class);

        return lpdf;
    }

    //-----------------------------------------------------------------------------------------------

}
