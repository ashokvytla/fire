package fire.nodes.streaming;

import fire.workflowengine.WorkflowContext;
import fire.workflowenginestreaming.NodeStreaming;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.regex.Pattern;

/**
 * Created by jayantshekhar on 11/16/15.
 */
public class NodeStreamingWordcount extends NodeStreaming {

    public NodeStreamingWordcount() {}

    public NodeStreamingWordcount(int i, String nm) {
        super(i, nm);
    }

    public void execute(JavaStreamingContext ssc, WorkflowContext workflowContext, JavaDStream<Row> dstream) {

        JavaDStream<Row> lineLengths = dstream.map(
                new Function<Row, Row>() {
                    @Override
                    public Row call(Row r) {
                        String string = r.getString(0);
                        String[] arr = string.split(" ");
                        int i = arr.length;

                        Row rr = RowFactory.create(i);
                        return rr;
                    }
                });

        lineLengths.print();

        super.execute(ssc, workflowContext, lineLengths);
    }

}
