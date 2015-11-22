package fire.examples.workflow.ml;

import fire.nodes.compactor.NodeSave;
import fire.nodes.dataset.NodeDatasetFileOrDirectoryText;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jayantshekhar on 11/12/15.
 */
public class WorkflowCompact {


    public static void main(String[] args) {

        // create spark and sql context
        JavaSparkContext ctx = CreateSparkContext.create(args);

        SQLContext sqlContext = new SQLContext(ctx);

        WorkflowContext workflowContext = new WorkflowContext();

        textwf(ctx, sqlContext, workflowContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void textwf(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext) {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetFileOrDirectoryText t = new NodeDatasetFileOrDirectoryText(1, "text node", "data/spam1.csv");
        wf.addNodeDataset(t);

        // node compact
        NodeSave compactTextFiles = new NodeSave(2, "compact");
        t.addNode(compactTextFiles);

        wf.execute(ctx, sqlContext, workflowContext);
    }

}
