package fire.nodes.solr;

import com.lucidworks.spark.SolrSupport;
import fire.util.spark.DataFrameUtil;
import fire.workflowengine.Node;
import fire.workflowengine.WorkflowContext;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

/**
 * Created by nikhil
 */
public class NodeSolrLoad extends Node {

    public String collection = "test";

    // dataframe id column
    public String idcol = "id";

    // dataframe columns
    public String dfcols = "c1 c2 c3";

    // solr columns
    public String solrcols = "s1 s2 s3";

    String zkHost = "";


    int queueSize = 1000;

    int numRunners = 2;

    int pollQueueTime = 20;

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
        final int[] dcolsidx = DataFrameUtil.getColumnIndexes(df, dfcols);

        // solr columns
        final String[] solrColsarr = this.solrcols.split(" ");

        // load the records into solr
        //df.toJavaRDD().map(new LoadRecordIntoSolr(dcolsidx));

        JavaPairRDD<String,SolrInputDocument> pairs = df.toJavaRDD().mapToPair(new PairFunction<Row, String, SolrInputDocument>() {

            public Tuple2<String, SolrInputDocument> call(Row row) throws Exception {
                SolrInputDocument doc = new SolrInputDocument();

                // get the values to be inserted from the row index
                for (int i = 0; i<dcolsidx.length; i++) {
                    doc.setField(solrColsarr[i], row.getString(dcolsidx[i]));
                }

                return new Tuple2<String, SolrInputDocument>((String) doc.getFieldValue(idcol), doc);
            }
        });

        SolrSupport.indexDocs(zkHost, collection, 100, pairs.values());

        // send a final commit in case soft auto-commits are not enabled
        CloudSolrClient cloudSolrClient = SolrSupport.getSolrServer(zkHost);
        cloudSolrClient.setDefaultCollection(collection);

        try {
            cloudSolrClient.commit(true, true);
            cloudSolrClient.close();
        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

}

