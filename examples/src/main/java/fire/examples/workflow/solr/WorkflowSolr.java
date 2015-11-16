package fire.examples.workflow.solr;

import fire.nodes.dataset.NodeDatasetFileOrDirectoryCSV;
import fire.nodes.ml.NodeKMeans;
import fire.nodes.solr.NodeSolrLoad;
import fire.sparkutil.CreateSparkContext;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jayant on 11/15/15.
 */
public class WorkflowSolr {


    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark and sql context
        JavaSparkContext ctx = CreateSparkContext.create(args);

        SQLContext sqlContext = new SQLContext(ctx);

        WorkflowContext workflowContext = new WorkflowContext();

        solrwf(ctx, sqlContext, workflowContext);

        // stop the context
        ctx.stop();
    }


    //--------------------------------------------------------------------------------------

    // kmeans workflow
    private static void solrwf(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext) {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetFileOrDirectoryCSV csv1 = new NodeDatasetFileOrDirectoryCSV(1, "csv1 node", "data/cars.csv",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");
        wf.addNodeDataset(csv1);

        // solr node
        NodeSolrLoad solr = new NodeSolrLoad(10, "solr load node", "f1 f2");
        csv1.addNode(solr);

        // execute the workflow
        wf.execute(ctx, sqlContext, workflowContext);

    }
}
