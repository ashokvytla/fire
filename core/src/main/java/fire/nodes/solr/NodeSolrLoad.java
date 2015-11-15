package fire.nodes.solr;

import fire.workflowengine.Node;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by nikhil
 */
public class NodeSolrLoad extends Node {

    public String collection = "test";

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


    }

}
