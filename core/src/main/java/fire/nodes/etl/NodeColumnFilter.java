package fire.nodes.etl;

import fire.dataframeutil.DataFrameUtil;
import fire.workflowengine.Node;
import fire.workflowengine.NodeDataset;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.collection.Seq;

/**
 * Created by nikhilshekhar on 13/11/15.
 */
public class NodeColumnFilter extends Node {

    //List of columns that are needed in the output dataframe
    public String columns = "label f1";

    public NodeColumnFilter() {}

    public NodeColumnFilter(int i, String nm) {
        super(i, nm);
    }

    public NodeColumnFilter(int i, String nm, String cols) {
        super(i, nm);

        columns = cols;
    }

    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df) {
        workflowContext.out("Executing NodeColumnFilter : " + id);
        Seq<Column> seq = DataFrameUtil.getColumnsAsSeq(df, columns);

        // select the required columns from the input dataframe
        DataFrame selectColumndf = df.select(seq);

        super.execute(ctx, sqlContext, workflowContext, selectColumndf);


    }
}
