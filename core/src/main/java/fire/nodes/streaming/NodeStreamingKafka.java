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
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jayantshekhar on 11/16/15.
 */
public class NodeStreamingKafka extends NodeStreaming {

    public String zkQuorum = "";
    public String group = "";
    public String topics = "";
    public int numThreads = 1;

    public NodeStreamingKafka() {}

    public NodeStreamingKafka(int i, String nm, String zkQ, String grp, String top, int numTh) {
        super(i, nm);

        zkQuorum = zkQ;
        group = grp;
        topics = top;
        numThreads = numTh;
    }

    @Override
    public void execute(JavaStreamingContext jssc, WorkflowContext workflowContext, JavaDStream<Row> dstream, FireSchema schema) {

        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topicsarr = topics.split(",");
        for (String topic: topicsarr) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
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
