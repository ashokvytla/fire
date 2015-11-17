package fire.workflowenginestreaming;

import fire.workflowengine.NodeSchema;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jayantshekhar on 11/16/15.
 */
public abstract class NodeStreaming  implements Serializable {


    // node id
    public int id;

    // node name
    public String name;

    // nodes list
    public List<NodeStreaming> nextNodes = new LinkedList<>();

    public void addNode(NodeStreaming node) {
        nextNodes.add(node);
    }

    public NodeStreaming()
    {

    }

    public NodeStreaming(int i, String nm) {
        id = i;
        name = nm;
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

    // execute the next nodes
    public void execute(JavaStreamingContext ctx, WorkflowContext workflowContext, JavaDStream<Row> dstream) {
        workflowContext.out("Executing node : " + id);

        Iterator<NodeStreaming> iterator = nextNodes.iterator();
        while (iterator.hasNext()) {
            NodeStreaming nextNode = iterator.next();
            nextNode.execute(ctx, workflowContext, dstream);
        }

    }

}
