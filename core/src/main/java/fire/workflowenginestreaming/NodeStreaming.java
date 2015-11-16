package fire.workflowenginestreaming;

import fire.workflowengine.NodeSchema;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jayantshekhar on 11/16/15.
 */
public abstract class NodeStreaming {


    // node id
    public int id;

    // node name
    public String name;

    // nodes list
    public List<NodeStreaming> nextNodes = new LinkedList<>();

    public void addNode(NodeStreaming node) {
        nextNodes.add(node);
    }

    //--------------------------------------------------------------------------------------

    // get the schema of a given node given the schema for this node
    public NodeSchema getSchema(int nodeId, NodeSchema currentSchema) {

        // return the incoming schema if the node id matches. nodes can override this behavior by implementing getSchema
        if (nodeId == this.id)
            return currentSchema;

        Iterator<NodeStreaming> iterator = nextNodes.iterator();
        while (iterator.hasNext()) {
            NodeStreaming nextNode = iterator.next();
            NodeSchema schema = nextNode.getSchema(nodeId, currentSchema);
            if (schema != null)
                return schema;
        }

        return null;
    }

    // execute the next nodes given the ougoing dataframe of this node
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {
        System.out.println("Executing node : "+id);

        Iterator<NodeStreaming> iterator = nextNodes.iterator();
        while (iterator.hasNext()) {
            NodeStreaming nextNode = iterator.next();
            nextNode.execute(ctx, sqlContext, workflowContext, df);
        }

    }

}
