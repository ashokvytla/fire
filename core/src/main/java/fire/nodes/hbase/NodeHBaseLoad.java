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
