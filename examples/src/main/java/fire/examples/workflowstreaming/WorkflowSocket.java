package fire.examples.workflowstreaming;

import fire.nodes.streaming.NodeStreamingSocketTextStream;
import fire.nodes.streaming.NodeStreamingWordcount;
import fire.sparkutil.CreateSparkContext;
import fire.workflowengine.WorkflowContext;
import fire.workflowenginestreaming.WorkflowStreaming;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by jayantshekhar on 11/16/15.
 */
public class WorkflowSocket {
    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark and sql context
        JavaStreamingContext ssc = CreateSparkContext.createStreaming(args);

        WorkflowContext workflowContext = new WorkflowContext();

        socketwf(ssc, workflowContext);

        ssc.start();
        ssc.awaitTermination();
    }


    //--------------------------------------------------------------------------------------

    // socket workflow
    private static void socketwf(JavaStreamingContext ctx, WorkflowContext workflowContext) {

        WorkflowStreaming wf = new WorkflowStreaming();

        // socket node
        NodeStreamingSocketTextStream stream = new NodeStreamingSocketTextStream(1, "streaming node");
        wf.addNodeDataset(stream);

        // streaming word count
        NodeStreamingWordcount wc = new NodeStreamingWordcount(2, "streaming word count");
        stream.addNode(wc);

        // execute the workflow
        wf.execute(ctx, workflowContext);

    }

}
