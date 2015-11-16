package fire.workflowenginestreaming;

import fire.nodes.dataset.NodeDataset;
import fire.workflowengine.NodeSchema;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;

/**
 * Created by jayantshekhar
 */
public class WorkflowStreaming {


    // the starting dataset nodes in the workflow. each subsequent node points to it next nodes
    public ArrayList<NodeStreaming> datasetNodes = new ArrayList<>();

    //--------------------------------------------------------------------------------------

    // get the schema for a given node id
    public NodeSchema getSchema(int nodeId) {
        for (NodeStreaming nodeDataset : datasetNodes) {
            NodeSchema schema = nodeDataset.getSchema(nodeId, null);
            if (schema != null)
                return schema;
        }

        return null;
    }

    //--------------------------------------------------------------------------------------

    // execute the workflow
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext) {
        for (NodeStreaming nodeDataset : datasetNodes) {
            nodeDataset.execute(ctx, sqlContext, workflowContext, null);
        }

    }

    //--------------------------------------------------------------------------------------

    // add dataset node to the workflow
    public void addNodeDataset(NodeStreaming node) {
        datasetNodes.add(node);
    }

}
