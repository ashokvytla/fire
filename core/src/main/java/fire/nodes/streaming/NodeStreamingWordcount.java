package fire.nodes.streaming;

import fire.workflowengine.Schema;
import fire.workflowengine.WorkflowContext;
import fire.workflowenginestreaming.NodeStreaming;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by jayantshekhar on 11/16/15.
 */
public class NodeStreamingWordcount extends NodeStreaming {
    public String col = "";

    public NodeStreamingWordcount() {}

    public NodeStreamingWordcount(int i, String nm, String c) {
        super(i, nm);
        col = c;
    }

    @Override
    public void execute(JavaStreamingContext ssc, WorkflowContext workflowContext,
                        JavaDStream<Row> dstream, Schema schema) {

        final int cidx = schema.getColIdx(col);

        JavaDStream<Row> lineLengths = dstream.map(
                new Function<Row, Row>() {
                    @Override
                    public Row call(Row r) {
                        String string = r.getString(cidx);
                        String[] arr = string.split(" ");
                        int i = arr.length;

                        Row rr = RowFactory.create(i);
                        return rr;
                    }
                });

        lineLengths.print();

        Schema newSchema = new Schema("count", "int", "numeric");

        super.execute(ssc, workflowContext, lineLengths, newSchema);
    }

}
