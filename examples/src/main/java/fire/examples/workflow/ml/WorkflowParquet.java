package fire.examples.workflow.ml;

import fire.nodes.dataset.NodeDatasetFileOrDirectoryParquet;
import fire.nodes.ml.*;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jayantshekhar on 11/8/15.
 */
public class WorkflowParquet {


    public static void main(String[] args) {

        // create spark and sql context
        JavaSparkContext ctx = CreateSparkContext.create(args);

        SQLContext sqlContext = new SQLContext(ctx);

        WorkflowContext workflowContext = new WorkflowContext();

        parquet(ctx, sqlContext, workflowContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void parquet(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext) {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetFileOrDirectoryParquet parquet = new NodeDatasetFileOrDirectoryParquet(1, "parquet node", "data/people.parquet");
        wf.addNodeDataset(parquet);

        // print first 3 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(2, "print first 3 rows", 3);
        parquet.addNode(nodePrintFirstNRows);

        // execute the workflow
        wf.execute(ctx, sqlContext, workflowContext);

    }

}
