package fire.nodes.streaming;

import fire.workflowengine.WorkflowContext;
import fire.workflowenginestreaming.NodeStreaming;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by jayantshekhar on 11/16/15.
 */
public class NodeStreamingSocketTextStream extends NodeStreaming {

    public NodeStreamingSocketTextStream() {}

    public NodeStreamingSocketTextStream(int i, String nm) {
        super(i, nm);
    }

    public void execute(JavaStreamingContext ctx, WorkflowContext workflowContext, JavaDStream<String> dstream) {

    }

}
