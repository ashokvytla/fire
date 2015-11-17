package fire.nodes.hbase;

import fire.workflowengine.Node;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by ashok
 */
public class NodeHBaseLoad extends Node {

    public String table = "test";
    public String colffam = "cf1";

    // dataframe columns
    public String dfcols = "c1 c2 c3";

    // hbase columns
    public String hbasecols = "h1 h2 h3";


    public NodeHBaseLoad() {
    }

    public NodeHBaseLoad(int i, String nm) {
        super(i, nm);
    }

    public NodeHBaseLoad(int i, String nm, String t) {
        super(i, nm);

        table = t;
    }

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {
        workflowContext.out("Executing NodeHBaseLoad : " + id);


    }

}
