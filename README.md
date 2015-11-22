### Docs Index

Doc  | 
------------- | -------------
[Developer Setup](https://github.com/FireProjects/fire/blob/master/docs/DeveloperSetup.md)  | 
[User Interface](https://github.com/FireProjects/fire/blob/master/docs/UserInterface.md)  | 
[Creating New Nodes](https://github.com/FireProjects/fire/blob/master/docs/CreatingNewNodes.md)  | 
[Nodes Backlog and Implemented List](https://github.com/FireProjects/fire/blob/master/docs/NodesBacklogAndImplemented.md)  | 
[Workflow Design](https://github.com/FireProjects/fire/blob/master/docs/WorkflowDesign.md)  |
[Streaming Workflow Design](https://github.com/FireProjects/fire/blob/master/docs/StreamingWorkflowDesign.md)  |
[Running Example Workflows on Hadoop/Spark Cluster](https://github.com/FireProjects/fire/blob/master/docs/RunningExampleWorkflowsOnHadoopCluster.md)  |


# Fire

Fire enables building end to end big data Applications for various Horizontals and Verticals on Apache Spark.
It does so by providing a workflow engine and a rich set of Nodes for various Big Data Functionality. Fire would also
provide a User Interface for building the workflows.

**The core design principle of Fire is to be lightweight, extensible, operational, allow building
User Interface/Dashboards and easy to use. And above all stay very close to programming in Spark and
avoid having many layers of code.**

Spark is the core computation engine which is supported. Any new computation node can be plugged
into the workflow. It supports data nodes, transform nodes, predictive modeling nodes, loading data into various stores
like hbase, solr etc. and above all schema propagation through the workflow.

Fire's core value preposition is to provide a number of Rich Nodes in an open framework that could be used out of the box
and thus enable much faster innovation and development of new use cases of Big Data.
Fire is Apache 2 Licensed http://www.apache.org/licenses/LICENSE-2.0.

## Big Data Applications

Though its various Nodes and out of the box workflows, Fire hopes to provide core functionality for building
Big Data Horizontal and Vertical Applications.

**Horizontal Applications**

  * IoT
  * Customer 360
  * Recommendation Engines
  * Analyzing logs
  * EDW Offload

**Verticals**

  * ECommerce
  * Telecom
  * Healthcare
  * Gaming


**Nodes and Workflows would be implemented in a Layered Framework**

<img src="https://github.com/FireProjects/fire/blob/master/docs/images/LayeredFunctionality.png"/>


## Building

	mvn package

## IntelliJ or Eclipse

Fire can be imported in IntelliJ or Scala IDE for Eclipse as a Maven project. Code can be developed and debugged within the IDE.

* https://www.jetbrains.com/idea/
* http://scala-ide.org/
 
Easiest way to get started it to run the example workflows under examples/src/main/java/fire/examples/workflow in your IDE.


## Examples

Example workflows are under examples. They are in the package fire.examples.workflow

https://github.com/FireProjects/fire/tree/master/examples/src/main/java/fire/examples/workflow

Example workflows include:


* **WorkflowKMeans** : k-means clustering
* **WorkflowLinearRegression** : linear regression
* **WorkflowLogisticRegression** : logistic regression
* **WorkflowALS** : ALS

More and more example workflows would keep getting added to the library.

## Running the example workflows on a Spark Cluster

Use the command below to load example data onto HDFS. It is then used by the example Workflows.

	hadoop fs -put data

Below are commands to run the various example Workflows on a Spark cluster. 

Executor memory of 2G, 4 executors with 2G each has been specified in the commands. The parameter **'cluster'** specifies that we are running the workflow on a cluster as against locally. This greatly simplifies the development and debugging within the IDE by setting its value to **'local'** or not specifying it.

	spark-submit --class fire.examples.workflow.ml.WorkflowKMeans --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

	spark-submit --class fire.examples.workflow.ml.WorkflowLinearRegression --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

	spark-submit --class fire.examples.workflow.ml.WorkflowLogisticRegression --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

	spark-submit --class fire.examples.workflow.ml.WorkflowParquet --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

## Creating your workflow

Workflows can be created in one of two ways:

* **Java** - Create the nodes, set their parameters and tie them with a workflow with code written in Java
* **JSON** - Create a json config file capturing the details of the various nodes and their connections. Then create a workflow object from it.

## Developers

The workflow engine is under core in the package **fire.workflowengine**.
The node implementations are under core in the package **fire.nodes**.

There are still a number of packages which are not used now but would be used in the future. Hence they can be safely ignored for now. So, its best to just focus on the above two at the moment.


## Architecture

The main entity is a workflow. A workflow contains nodes connected to each other. Nodes also have parameters
which are set. Data is passed from one node to another as Spark **DataFrame**. The output DataFrame of a Node can have a different Schema from its input DataFrame. A Node can add or remove columns from a DataFrame.


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

## Workflow Execution

The diagram below shows the execution flow.


<img src="https://github.com/FireProjects/fire/blob/master/docs/images/Architecture.png"/>


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


## Node

Any Node receives Dataframes as inputs and produces Dataframes as outputs. Every node has an 'execute' method with
the following signature:

	public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df)

A Predictive Node can also produce a Model as output. If it is connected to a Scoring Node, it passes along the Model
to the Scoring Node.


The execute method in Node() passes along the dataframe to the next node.

So after execution in general, the Nodes call Node.execute() to pass along the execution flow and the new dataframe
produced to the next node in the workflow.

Details for creating new nodes can be found here : https://github.com/FireProjects/fire/blob/master/docs/CreatingNewNodes.md


## Schema Propagation

Workflow supports Schema Propagation. The method getSchema(nodeid) returns the Schema for the given node id. Each Node supports the method

	public MySchema getSchema(int nodeId, MySchema prevSchema)

'nodeId' is the id of the node for which the schema is being asked for. 'prevSchema' is the output schema from the
previous node. The node then uses the incoming schema to form its output schema. If the nodeId matches the current node
id, it returns the new schema. If not, it passes the new schema also to its next node.

getSchema() method in Node by default propagates the incoming schema to the outgoing Nodes. It can be overridden by
the specific Nodes. For example NodeJoin adds the various incoming schemas to generate the output schema.

NodeSchema represents the schema of a node.

https://github.com/FireProjects/fire/blob/master/core/src/main/java/fire/workflowengine/NodeSchema.java

## WorkflowContext

WorkflowContext is passed to the Node execute method.

The Nodes output things like Logs, Results (can be graphs), Schema to the WorkflowContext. Based on the Application,
there would be various implementations of the WorkflowContext. An example of it would be BrowserStreamingWorkflowContext. It would stream the results back to the Browser when used with a WebServer. The result would appropriately get displayed in the Browser in various tabs.

https://github.com/FireProjects/fire/blob/master/core/src/main/java/fire/workflowengine/WorkflowContext.java

## WorkflowMetrics

WorkflowMetrics has not yet been implemented.

## Nodes Implemented

The following Nodes have been implemented till now. The comprehensive list is being maintained
here : https://github.com/FireProjects/fire/blob/master/docs/NodesBacklogAndImplemented.md

They reside under :

https://github.com/FireProjects/fire/tree/master/core/src/main/java/fire/nodes

#### Dataset Nodes

* NodeDatasetFileOrDirectoryCSV.java : Reads in a CSV file
* NodeDatasetFileOrDirectoryParquet.java : Reads in a Parquet file


#### Predictive Modeling Nodes

* **NodeLinearRegression.java** : Linear Regression
* **NodeLinearRegressionWithSGD.java** : Linear Regression with SGD
* **NodeLogisticRegression.java** : Logistic Regression
* **NodeDecisionTree.java** : Decision Tree
* **NodeDatasetSplit.java** : Splits an incoming dataset for train and test
* **NodeKMeans.java** : KMeans Clustering
* **NodeALS.java** : ALS
* **NodeModelScore.java** : Scores a given model and test dataset
* **NodeSummaryStatistics.java** : Summary Statistics

#### ETL Nodes

* **NodeJoin.java** : Joins the incoming datasets on the given keys

#### File Ingestion Nodes

* **CompactTextFiles.java** : Compacts a set of small text files into larger ones

#### Utility Nodes

* **NodePrintFirstNRows.java** : Prints the first N rows of a dataset

## Nodes to be built

The backlog of Nodes are at : https://github.com/FireProjects/fire/blob/master/docs/NodesBacklogAndImplemented.md

## User Interface

UI has not been built yet, but would be a great addition to the project. The UI would first provide the following:

* Creation and Saving of Workflows with Nodes
* Execution of the workflow from the UI
* Display of the workflow execution logs, results and graphs in the UI

jsplumb would be great for building the workflows.

https://jsplumbtoolkit.com/


## Contributing

Do feel free to send in Pull requests. Best way to get started is to send in Pull request for new Nodes that implement new functionality.






