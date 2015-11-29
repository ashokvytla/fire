package fire.nodes.solr;

import com.lucidworks.spark.SolrRDD;
import com.lucidworks.spark.SolrSupport;
import fire.util.spark.DataFrameUtil;
import fire.workflowengine.FireSchema;
import fire.workflowengine.Node;
import fire.workflowengine.WorkflowContext;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by nikhil
 */
public class NodeSolrQuery extends Node {

    public String collection = "test";

    // dataframe id column
    public String idcol = "id";

    // dataframe columns
    public String dfcols = "c1 c2 c3";

    // solr columns
    public String solrcols = "s1 s2 s3";

    String zkHost = "localhost:9983";


    public NodeSolrQuery() {}

    public NodeSolrQuery(int i, String nm) {
        super(i, nm);
    }

    public NodeSolrQuery(int i, String nm, String c) {
        super(i, nm);

        collection = c;
    }


    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {
        workflowContext.out("Executing NodeSolrQuery : " + id);

        String queryStr = "*:*";

        SolrRDD solrRDD = new SolrRDD(zkHost, collection);
        final SolrQuery solrQuery = SolrRDD.toQuery(queryStr);
        JavaRDD<SolrDocument> solrJavaRDD = null;
        try {
            solrJavaRDD = solrRDD.query(ctx.sc(), solrQuery);
        } catch(Exception ex) {
            workflowContext.out(ex);
        }

        // solr columns
        final String[] solrColsarr = this.solrcols.split(" ");

        JavaRDD<Row> rowRDD = solrJavaRDD.flatMap(new FlatMapFunction<SolrDocument, Row>() {
            public Iterable<Row> call(SolrDocument doc) {

                Object f[] = new Object[solrColsarr.length];
                int idx = 0;
                for (String field : solrColsarr) {
                    f[idx] = doc.get(field);
                    idx++;
                }

                Row row = RowFactory.create(f);

                return Arrays.asList(row);

            }
        });

        // get schema
        final StructType schema = getSparkSQLSchema();

        // Apply the schema to the RDD.
        DataFrame tdf = sqlContext.createDataFrame(rowRDD, schema);

        super.execute(ctx, sqlContext, workflowContext, tdf);

    }

    // get the spark sql schema
    public StructType getSparkSQLSchema() {
        final StructType schema = new FireSchema(solrcols, "", "").getSparkSQLStructType();

        return schema;
    }

}

