# Creating New Nodes

New Nodes can be easily added for adding new functionality.

## Check out the Existing Nodes

* Check out the various groups of Nodes at https://github.com/FireProjects/fire/tree/master/core/src/main/java/fire/nodes

* Some examples of various Nodes:
    * Read in Parquet Files : https://github.com/FireProjects/fire/blob/master/core/src/main/java/fire/nodes/dataset/NodeDatasetFileOrDirectoryParquet.java
    * LinearRegression : https://github.com/FireProjects/fire/blob/master/core/src/main/java/fire/nodes/ml/NodeLinearRegression.java
    * Join : https://github.com/FireProjects/fire/blob/master/core/src/main/java/fire/nodes/etl/NodeJoin.java

## Create the New Node

* Create the New Node as a Java class by extending one of the following classes
    * **NodeDataset** : Dataset Nodes
    * **NodeModeling** : Predictive Modeling Nodes
    * **NodeETL** : ETL Nodes
    * **NodeGraph** : Graph Nodes
    * **NodeHbase** : HBase Nodes
    * **NodeStreaming** : Streaming Nodes

## Overide the execute() method in the New Node:

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df)

The execute method takes in the following parameters:

* JavaSparkContext
* SQLContext
* WorkflowContext : Used to output items of result from the Node to the frameworkd
* DataFrame : The incoming dataset into the Node. Dataset Nodes which produce output only do not take in any input DataFrame.


## Overide the getSchema() method in the new Node

If the new Node changes the incoming schema, override the getSchema() method to return the new output schema from the node.

    @Override
    public NodeSchema getSchema(int nodeId, NodeSchema previousSchema) {


