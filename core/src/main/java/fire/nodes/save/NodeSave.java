package fire.nodes.save;

import fire.workflowengine.Node;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

/**
 * Created by jayantshekhar on 11/16/15.
 */
public class NodeSave extends Node implements Serializable {
    public String path;

    public NodeSave() {}

    public NodeSave(int i, String nm) {
        super(i, nm);
    }

    public NodeSave(int i, String nm, String p) {
        super(i, nm);

        path = p;
    }

    //------------------------------------------------------------------------------------------------------

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {
        workflowContext.out("Executing NodeSave : "+id);

        df.saveAsParquetFile(path);

        super.execute(ctx, sqlContext, workflowContext, df);
    }

}
