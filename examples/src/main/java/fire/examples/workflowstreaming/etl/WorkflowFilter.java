package fire.examples.workflowstreaming.etl;

import fire.nodes.dataset.NodeDatasetFileOrDirectoryCSV;
import fire.nodes.etl.NodeColumnFilter;
import fire.nodes.ml.NodeKMeans;
import fire.nodes.ml.NodePrintFirstNRows;
import fire.sparkutil.CreateSparkContext;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jayantshekhar on 11/13/15.
 */
public class WorkflowFilter {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark and sql context
        JavaSparkContext ctx = CreateSparkContext.create(args);

        SQLContext sqlContext = new SQLContext(ctx);

        WorkflowContext workflowContext = new WorkflowContext();

        filterwf(ctx, sqlContext, workflowContext);

        // stop the context
        ctx.stop();
    }


    //--------------------------------------------------------------------------------------

    // filter columns workflow workflow
    private static void filterwf(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext) {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetFileOrDirectoryCSV csv1 = new NodeDatasetFileOrDirectoryCSV(1, "csv1 node", "data/cars.csv",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");
        wf.addNodeDataset(csv1);

        // column filter node
        NodeColumnFilter filter = new NodeColumnFilter(2, "filter node", "f1 f2");
        csv1.addNode(filter);

        // print first 2 rows
        NodePrintFirstNRows printFirstNRows = new NodePrintFirstNRows(3, "print first rows", 2);
        filter.addNode(printFirstNRows);

        // execute the workflow
        wf.execute(ctx, sqlContext, workflowContext);

    }
    
}
