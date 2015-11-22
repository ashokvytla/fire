package fire.nodes.streaming;

import fire.workflowengine.Schema;
import fire.workflowengine.WorkflowContext;
import fire.workflowenginestreaming.NodeStreaming;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by jayantshekhar on 11/16/15.
 */
public class NodeStreamingSocketTextStream extends NodeStreaming {

    public String hostname = "localhost";
    public int port = 9999;

    public NodeStreamingSocketTextStream() {}

    public NodeStreamingSocketTextStream(int i, String nm) {
        super(i, nm);
    }

    @Override
    public void execute(JavaStreamingContext ssc, WorkflowContext workflowContext, JavaDStream<Row> dstream, Schema schema) {

        // Create a JavaReceiverInputDStream on target ip:port
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                hostname, 9999, StorageLevels.MEMORY_AND_DISK_SER);

        // map the DStream of String to that of Row
        JavaDStream<Row> linesRow = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String x) {
                return RowFactory.create(x);
            }
        });

        linesRow.print();

        Schema outSchema = new Schema("message", "string", "text");

        super.execute(ssc, workflowContext, linesRow, outSchema);
    }

}
