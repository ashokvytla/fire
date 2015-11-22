package fire.nodes.hbase;

import com.cloudera.spark.hbase.JavaHBaseContext;
import fire.dataframeutil.DataFrameUtil;
import fire.workflowengine.Node;
import fire.workflowengine.WorkflowContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;


/**
 * Created by Ashok Rajan on 11/16/15.
 */
public class HBaseNode extends Node implements Serializable {

    public String hbaseTableName = "person";

    public String hbaseColumnFamily = "persondetails";

    // Dataframe columns
    public String dfCols = "id fn age city";

    // HBase columns
    public String hbaseCols = "personid fullname personage personcity";

    public HBaseNode(int i, String nm) {
        super(i, nm);
    }

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {

        workflowContext.out("Executing HBaseNode : " + id);

        String[] columns = df.columns();

        // find the indexes of the data frame columns
        final int[] dcolsidx = DataFrameUtil.getColumnIndexes(df, dfCols);

        bulkPutHBaseContext(ctx, df);

        super.execute(ctx, sqlContext, workflowContext, df);
    }

    //TODO: Mapping to HBase columns to columns in DataFrame with datatype
    private void bulkPutHBaseContext(JavaSparkContext javaSparkContext, DataFrame dataFrame) {

        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(new Path("/etc/hbase/conf.cloudera.hbase/hbase-site.xml"));
        JavaHBaseContext javaHBaseContext = new JavaHBaseContext(javaSparkContext, configuration);
        JavaRDD rdd = dataFrame.toJavaRDD();
        javaHBaseContext.bulkPut(rdd, hbaseTableName, new Function<Row, Put>() {
                    private static final long serialVersionUID = 1L;

                    public Put call(Row row) throws Exception {
                        Put put = new Put(Bytes.toBytes(row.getString(0)));
                        put.addColumn(Bytes.toBytes(hbaseColumnFamily), Bytes.toBytes("fullname"), Bytes.toBytes(row.getString(1)));
                        put.addColumn(Bytes.toBytes(hbaseColumnFamily), Bytes.toBytes("personage"), Bytes.toBytes(row.getString(2)));
                        put.addColumn(Bytes.toBytes(hbaseColumnFamily), Bytes.toBytes("personcity"), Bytes.toBytes(row.getString(3)));
                        return put;
                    }
                },
                true);

    }
}


class LoadMappedRecordIntoHBase implements Function<Row, String> {

    int[] dcolsidx;

    LoadMappedRecordIntoHBase(int[] colsidx) {
        dcolsidx = colsidx;
    }

    public String call(Row r) {
        System.out.println( r.toString());

        // array of values
        String[] validx = new String[dcolsidx.length];

        // get the values to be inserted from the row index
        for (int i = 0; i<dcolsidx.length; i++) {
            validx[i] = r.getString(dcolsidx[i]);
            System.out.println( validx[i]);
        }

        // insert the record into solrRow
        return r.toString();
    }
}


class println implements Function<String,String> {

    public String call(String r) {
        System.out.println( r);

        // insert the record into solr
        return "testing";

    }
}


