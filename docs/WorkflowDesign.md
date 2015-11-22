# Workflow Design

Workflow consists on Nodes connected to each other. Workflows get executed on Spark.

## Architecture

The main entity is a workflow. A workflow contains nodes connected to each other. Nodes also have parameters
which are set. Data is passed from one node to another as Spark **DataFrame**. The output DataFrame of a Node can
have a different Schema from its input DataFrame. A Node can add or remove columns from a DataFrame.

<img src="https://github.com/FireProjects/fire/blob/master/docs/images/Architecture.png"/>

<img src="https://github.com/FireProjects/fire/blob/master/docs/images/Workflow.png"/>

Nodes can be:

* **Dataset nodes** which creates the DataFrame from some store for the rest of the nodes to act upon.
* **Transform nodes** which process the incoming dataset/s to produce another dataset.
* **Modeling nodes** which apply a predictive algorithm on the incoming dataset to produce a model
* **Scoring nodes** which take in a dataset and and model and score it.
* **Decision nodes** which take in a dataset, compute some value and pass on the execution to one of its connected outputs.
* **Split nodes** which take in a dataset and split it into subsets and pass on the execution to its outputs with the split datasets.
* **ETL nodes** which operate on source datasets and perform common ETL operations.
* **Save nodes** which save the datasets onto HDFS.

### Workflow

**fire.workflowengine.Workflow** represents the workflow.

It contains an array of dataset nodes which ingest or load data from some store like HDFS/HBase etc.

    public ArrayList<NodeDataset> datasetNodes = new ArrayList<>();

It has an execute() method which executes the workflow.

    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext)

It also has a getOutputSchema() method which returns the output schema of a given node id. This would also be used
in the User Interface to display the fields where needed.

    public FireSchema getOutputSchema(int nodeId)


## Node

Any Node receives Dataframes as inputs and produces Dataframes as outputs. Every node has an 'execute' method with
the following signature:

	public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df)

More details on Spark Dataframe is available here : http://spark.apache.org/docs/latest/sql-programming-guide.html#dataframes

A Predictive Node can also produce a Model as output. If it is connected to a Scoring Node, it passes along the Model
to the Scoring Node.

The execute method in Node() passes along the dataframe to the next node.

So after execution in general, the Nodes call Node.execute() to pass along the execution flow and the new dataframe
produced to the next node in the workflow.

Specific Node classes provide the base class for various groupings of nodes.

* **Node**
	* NodeModeling
	* NodeDataset
	* NodeHBase
	* NodeETL
	* NodeGraph
	* NodeSave
	* NodeSolr

Details on creating new nodes can be found here : https://github.com/FireProjects/fire/blob/master/docs/CreatingNewNodes.md

## Fire Schema

Schema is represented by the class **fire.workflowengine.FireSchema**

It has the following fields:

    * public String[] columnNames; // names of the different columns
    * public org.apache.avro.Schema.Type[] columnTypes; : avro Type of each columns
    * public int[] columnMLTypes; // Machine Learning type of the column which can be numeric/categorical/string

    

## Schema Propagation

Workflow supports Schema Propagation. The method getSchema(nodeid) returns the Schema for the given node id. Each Node supports the method

	public FireSchema getOutputSchema(int nodeId, FireSchema inputSchema)

'nodeId' is the id of the node for which the schema is being asked for. 'inputSchema' is the input schema to this node.
The node then uses the input schema to form its output schema. If the nodeId matches the current node
id, it returns the new schema. If not, it passes the new schema also to its next node.

getSchema() method in Node by default propagates the incoming schema to the outgoing Nodes. It can be overridden by
the specific Nodes. For example NodeJoin adds the various incoming schemas to generate the output schema.

NodeSchema represents the schema of a node.

https://github.com/FireProjects/fire/blob/master/core/src/main/java/fire/workflowengine/NodeSchema.java



## JSON representation of the workflow

A workflow can be saved to a json structure into a file or can be created from one. This allows for:

* Driving user interfaces to build the workflow and save it to a file/rdbms.
* Exchanging workflows.
* Hand build a workflow or update the node parameters in a JSON file.
* Execute a workflow JSON file in Spark with the workflow driver.

## Workflow User Interface

There would be a browser based User Interface to build workflows. It would take in a text file representation of the
various nodes and their parameters. It would allow users to create a workflow using the UI, set the parameters for
the various nodes and save it. It would also allow users to execute a workflow from the UI and view the results.

## Graphs

When a node is executed, it may also produce graphs as output. This output is streamed back to the browser and displayed.


## WorkflowContext

WorkflowContext is passed to the Node execute method.

The Nodes output things like Logs, Results (can be graphs), Schema to the WorkflowContext. Based on the Application,
there would be various implementations of the WorkflowContext. An example of it would be BrowserStreamingWorkflowContext. It would stream the results back to the Browser when used with a WebServer. The result would appropriately get displayed in the Browser in various tabs.

https://github.com/FireProjects/fire/blob/master/core/src/main/java/fire/workflowengine/WorkflowContext.java

## WorkflowMetrics

WorkflowMetrics has not yet been implemented.



