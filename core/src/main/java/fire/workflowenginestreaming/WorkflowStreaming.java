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
    public ArrayList<NodeDataset> datasetNodes = new ArrayList<>();

    //--------------------------------------------------------------------------------------

    // get the schema for a given node id
    public NodeSchema getSchema(int nodeId) {
        for (NodeDataset nodeDataset : datasetNodes) {
            NodeSchema schema = nodeDataset.getSchema(nodeId, null);
            if (schema != null)
                return schema;
        }

        return null;
    }

    //--------------------------------------------------------------------------------------

    // check if there is any circular traversal in the workflow
    // tries to see if it runs into > 500 nodes when traversing. we assume that we would not have more than
    // 500 nodes in any workflow
    // WE ARE CURRENTLY USING THIS ONE AS AGAINST isCircular()
    public boolean isTraversalCircular() {

        Integer numNodesVisited = 0;

        for (NodeDataset nodeDataset : datasetNodes) {
            boolean result = nodeDataset.isTraversalCircular(numNodesVisited);
            if (result)
                return true;
        }

        return false;
    }

    //--------------------------------------------------------------------------------------

    // execute the workflow
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext) {
        for (NodeDataset nodeDataset : datasetNodes) {
            nodeDataset.execute(ctx, sqlContext, workflowContext, null);
        }

    }

    //--------------------------------------------------------------------------------------

    // add dataset node to the workflow
    public void addNodeDataset(NodeDataset node) {
        datasetNodes.add(node);
    }

}
