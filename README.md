# Fire

Fire enables building end to end big data Applications for various Horizontals and also for the various Verticals.

It does so by providing a framework for building and running workflows. At this time Spark is the core execution
engine which would be supported. Any new computation node can be plugged into the workflow. It supports data nodes,
transform nodes, nodes that build predictive models and above all schema propagation through the workflow.

**Horizontal Apps**

  * Analyzing logs
  * EDW Offload
  * IoT
  * Recommendation Engines

**Vertical Apps**

  * ECommerce
  * Telecom
  * Healthcare

## Building

	mvn package


## Run

	spark-submit --class com.RunCount --master yarn-client --executor-memory 15G  --num-executors 4  --executor-cores 3  fire-parent-1.2.0-SNAPSHOT-jar-with-dependencies.jar

## Examples

Workflow examples are under examples. Files are in the package fire.nodes.examples under the project examples


## Creating your workflow

Workflows can be created in one of two ways:

* Create the nodes, set their parameters and tie them with a workflow with code written in Java/Scala
* Create a json config file capturing the details of the various nodes and their connections

## Developers

The worflow engine is under core in the package fire.workflowengine.
The node implementations are under core in the package fire.nodes

There are still a number of packages which are not used now but would be used in the future. Hence they can be safely ignored for now.
So, its best to just focus on the above two at the moment.


## Architecture

The main entity is a workflow. A workflow contains nodes connected to each other. Nodes also have parameters
which are set. Nodes can be:

* Starting nodes which are mainly data nodes and produce data for the rest of the nodes to act upon.
* Transform nodes which process the incoming dataset/s to produce another dataset.
* Modeling nodes which apply a predictive algorithm on the incoming dataset to produce a model
* Scoring nodes which take in a dataset and and model and score it.
* Decision nodes which take in a dataset, compute some value and pass on the execution to one of its connected outputs.
* Split nodes which take in a dataset and split it into subsets and pass on the execution to its outputs with the split datasets.

## JSON representation of the workflow

A workflow can be saved to a json structure into a file or can be created from one. This allows for:

* Driving user interfaces to build the workflow and save it to a file/rdbms.
* Exchanging workflows.
* Hand build a workflow or update the node parameters in a JSON file.
* Execute a workflow JSON file in Spark with the workflow driver.

## Workflow User Interface

There would a browser based User Interface to build workflows. It would take in a text file representation of the various nodes and their parameters.
It would allow users to create a workflow using the UI, set the parameters for the various nodes and save it.
It would also allow users to execute a workflow from the UI and view the results.

## Graphs

When a node is executed, it may also produce graphs as output. This output is streamed back to the browser and displayed.


## Writing a New Node

Any Node received Dataframes as inputs and produce Dataframes as outputs. Every node has an 'execute' method with the
following signature:

public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df)

A Predictive Node can also produce a Model as output. If it is connected to a Scoring Node, it passes along the Model
to the Scoring Node.


The execute method in Node() passes along the dataframe to the next node.

public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df)

So after execution in general, the Nodes call Node.execute() to pass along the new dataframe produced to the next node
in the workflow.

## Schema Propagation

Workflow supports Schema Propagation. Each Node supports the method

public MySchema getSchema(int nodeId, MySchema sch)


getSchema() method in Node by default propagates the incoming schema to the outgoing Nodes. It can be overridden by
the specific Nodes. For example NodeJoin adds the various incoming schemas to generate the output schema.


## WorkflowContext

WorkflowContext is passed to the Node execute method.

The Nodes output things like Results, Logs, Schema to the WorkflowContext. Based on the Application various Classes
would extent WorkflowContext. An example of it would be BrowserStreamingWorkflowContext. It would stream the results
back to the Browser when used with a WebServer. It would appropriately get displayed in the Browser.

## WorkflowMetrics

WorkflowMetrics has not yet been implemented.

## Nodes Implemented

The following Nodes have been implemented till now.

**Dataset Nodes**

* NodeDatasetFileOrDirectoryCSV.java : Reads in a CSV file
* NodeDatasetFileOrDirectoryParquet.java : Reads in a Parquet file


**Predictive Modeling Nodes**

* NodeLinearRegression.java : Linear Regression
* NodeLinearRegressionWithSGD.java : Linear Regression with SGD
* NodeLogisticRegression.java : Logistic Regression
* NodeDecisionTree.java : Decision Tree
* NodeDatasetSplit.java : Splits an incoming dataset for train and test
* NodeKMeans.java : KMeans Clustering
* NodeALS.java : ALS
* NodeModelScore.java : Scores a given model and test dataset
* NodeSummaryStatistics.java : Summary Statistics

**Utility Nodes**

* NodePrintFirstNRows.java : Prints the first N rows of a dataset
* NodeJoin.java : Joins the incoming datasets on the given keys

**File Ingestion**

* CompactTextFiles.java : Compacts a set of small text files into larger ones







