package fire.nodes.solr;

import fire.workflowengine.Node;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Created by nikhil
 */
public class NodeSolrLoad extends Node {

    public String collection = "test";

    // dataframe columns
    public String dfcols = "c1 c2 c3";

    // solr columns
    public String solrcols = "s1 s2 s3";

    public NodeSolrLoad() {}

    public NodeSolrLoad(int i, String nm) {
        super(i, nm);
    }

    public NodeSolrLoad(int i, String nm, String c) {
        super(i, nm);

        collection = c;
    }


    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {
        workflowContext.out("Executing NodeSolrLoad : " + id);

        // columns in the dataframe
        String[] columns = df.columns();

        // find the indexes of the data frame columns
        final String[] dcols = dfcols.split(" ");
        final int[] dcolsidx = new int[dcols.length];

        for (int i=0; i<dcols.length; i++) {
            for (int j=0; j<columns.length; j++) {
                if (dcols[i].equals(columns[j]))
                    dcolsidx[i] = j;
            }
        }


        df.toJavaRDD().map(new LoadRecordIntoSolr(dcolsidx));
    }

}

class LoadRecordIntoSolr implements Function<Row, Row> {

    int[] dcolsidx;

    LoadRecordIntoSolr(int[] colsidx) {
        dcolsidx = colsidx;
    }

    public Row call(Row r) {

        // array of values
        String[] validx = new String[dcolsidx.length];

        // get the values to be inserted from the row index
        for (int i = 0; i<dcolsidx.length; i++) {
            validx[i] = r.getString(dcolsidx[i]);
        }

        // insert the record into solr

        return null;
    }
}
