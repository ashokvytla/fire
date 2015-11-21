# Creating New Nodes

New Nodes can be easily added for adding new functionality.

## Review and Design the New Node

* Check out the various groups of Nodes at https://github.com/FireProjects/fire/tree/master/core/src/main/java/fire/nodes
* Determine whether the New Node belongs to dataset/etl/graph/ml set of nodes.

* Some examples of various Nodes:
    * Read in Parquet Files : https://github.com/FireProjects/fire/blob/master/core/src/main/java/fire/nodes/dataset/NodeDatasetFileOrDirectoryParquet.java
    * LinearRegression : https://github.com/FireProjects/fire/blob/master/core/src/main/java/fire/nodes/ml/NodeLinearRegression.java
    * Join : https://github.com/FireProjects/fire/blob/master/core/src/main/java/fire/nodes/etl/NodeJoin.java

## Create the New Node

* Create the New Node as a Java class
* Dataset Nodes extend the class NodeDataset, Modeling Nodes extend the class NodeModeling.

## Overide the execute method in the Node:

    @Override
    public void execute(JavaSparkContext ctx, SQLContext sqlContext, WorkflowContext workflowContext, DataFrame df)

The execute method takes in the following parameters:

* JavaSparkContext
* SQLContext
* WorkflowContext : Used to output items of result from the Node to the frameworkd
* DataFrame : The incoming dataset into the Node. Dataset Nodes which produce output only do not take in any input DataFrame.



