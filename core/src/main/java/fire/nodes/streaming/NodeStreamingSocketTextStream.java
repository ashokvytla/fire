package fire.nodes.streaming;

import com.google.common.collect.Lists;
import fire.workflowengine.WorkflowContext;
import fire.workflowenginestreaming.NodeStreaming;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.*;

import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * Created by jayantshekhar on 11/16/15.
 */
public class NodeStreamingSocketTextStream extends NodeStreaming {

    private static final Pattern SPACE = Pattern.compile(" ");

    public NodeStreamingSocketTextStream() {}

    public NodeStreamingSocketTextStream(int i, String nm) {
        super(i, nm);
    }

    public void execute(JavaStreamingContext ssc, WorkflowContext workflowContext, JavaDStream<Row> dstream) {

        // Create a JavaReceiverInputDStream on target ip:port and count the
        // words in input stream of \n delimited text (eg. generated by 'nc')
        // Note that no duplication in storage level only for running locally.
        // Replication necessary in distributed scenario for fault tolerance.
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                "localhost", Integer.parseInt("9999"), StorageLevels.MEMORY_AND_DISK_SER);

        JavaDStream<Row> linesRow = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String x) {
                return RowFactory.create(x);
            }
        });

        linesRow.print();

        super.execute(ssc, workflowContext, linesRow);
    }

}
