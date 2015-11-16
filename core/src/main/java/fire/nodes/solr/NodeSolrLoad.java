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

        df.toJavaRDD().map(new LoadRecordIntoSolr());
    }

}

class LoadRecordIntoSolr implements Function<Row, Row> {

    public Row call(Row r) {
        r.getString(0);

        return null;
    }
}
