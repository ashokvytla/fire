package fire.nodes.streaming;

import fire.workflowengine.FireSchema;
import fire.workflowengine.WorkflowContext;
import fire.workflowenginestreaming.NodeStreaming;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jayantshekhar on 11/16/15.
 */
public class NodeStreamingFlume extends NodeStreaming {

    public String host = "";
    public int port = 1;

    public NodeStreamingFlume() {}

    public NodeStreamingFlume(int i, String nm, String h, int p) {
        super(i, nm);

        host = h;
        port = p;
    }

    @Override
    public void execute(JavaStreamingContext jssc, WorkflowContext workflowContext, JavaDStream<Row> dstream, FireSchema schema) {

        JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createStream(jssc, host, port);

        JavaDStream<String> lines = flumeStream.map(new Function<SparkFlumeEvent, String>() {
            @Override
            public String call(SparkFlumeEvent event) {
                return event.event().getBody().toString();
            }
        });


        // map the DStream of String to that of Row
        JavaDStream<Row> linesRow = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String x) {
                return RowFactory.create(x);
            }
        });

        linesRow.print();

        FireSchema outSchema = new FireSchema("message", "string", "text");

        super.execute(jssc, workflowContext, linesRow, outSchema);
    }

}
