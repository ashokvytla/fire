package fire.workflowenginestreaming;

import fire.workflowengine.Schema;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.ArrayList;

/**
 * Created by jayantshekhar
 * Workflow for Spark Streaming
 */
public class WorkflowStreaming {


    // the starting dataset nodes in the workflow. each subsequent node points to it next nodes
    public ArrayList<NodeStreaming> datasetNodes = new ArrayList<>();

    //--------------------------------------------------------------------------------------

    // get the schema for a given node id
    public Schema getSchema(int nodeId) {
        for (NodeStreaming nodeDataset : datasetNodes) {
            Schema schema = nodeDataset.getSchema(nodeId, null);
            if (schema != null)
                return schema;
        }

        return null;
    }

    //--------------------------------------------------------------------------------------

    // execute the workflow
    public void execute(JavaStreamingContext ctx, WorkflowContext workflowContext) {

        // execute all the streaming dataset nodes
        for (NodeStreaming nodeDataset : datasetNodes) {
            nodeDataset.execute(ctx, workflowContext, null, null);
        }

    }

    //--------------------------------------------------------------------------------------

    // add dataset node to the workflow
    public void addNodeDataset(NodeStreaming node) {
        datasetNodes.add(node);
    }

}
