package fire.examples.workflowstreaming;

import fire.nodes.dataset.NodeDatasetFileOrDirectoryCSV;
import fire.nodes.ml.NodeKMeans;
import fire.nodes.streaming.NodeStreamingSocketTextStream;
import fire.sparkutil.CreateSparkContext;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import fire.workflowenginestreaming.WorkflowStreaming;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by jayantshekhar on 11/16/15.
 */
public class WorkflowSocket {
    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark and sql context
        JavaStreamingContext ctx = CreateSparkContext.createStreaming(args);

        WorkflowContext workflowContext = new WorkflowContext();

        socketwf(ctx, workflowContext);

        // stop the context
        ctx.stop();
    }


    //--------------------------------------------------------------------------------------

    // socket workflow
    private static void socketwf(JavaStreamingContext ctx, WorkflowContext workflowContext) {

        WorkflowStreaming wf = new WorkflowStreaming();

        // socket node
        NodeStreamingSocketTextStream stream = new NodeStreamingSocketTextStream(1, "streaming node");
        wf.addNodeDataset(stream);


        // execute the workflow
        wf.execute(ctx, workflowContext);

    }

}
