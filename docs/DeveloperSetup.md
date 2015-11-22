# Developer Setup

Fire is mainly written in Java and a bit in Scala. It uses maven. The parent pom has 2 modules at this time:

* core
* examples

The number of modules would grow over time with things like customer 360, recommendations, various verticals etc.
getting added.

## Prerequisites

Please make sure you have Java, Maven and git installed in your development environment.

## Spark Version

The current pom.xml uses Spark 1.5 which is part of Cloudera CDH 5.5 distribution.

## Checking out the code with Git

	git clone https://github.com/FireProjects/fire.git


## Building with maven

	mvn package


## Using an IDE

IntelliJ or Scala IDE for Eclipse can be used for developing and debugging Fire.

### Importing into IntelliJ

IntelliJ can be downloaded from https://www.jetbrains.com/idea/

* Add the scala plugin into IntelliJ.
* Then import the project as a Maven project into IntelliJ.
* Start with executing the example workflows. They do not need any argument for running in local mode.


### Importing into Scala IDE for Eclipse

Fire can be imported into Scala IDE for Eclipse as a Maven project.

http://scala-ide.org/

Easiest way to get started it to run the example workflows under examples/src/main/java/fire/examples/workflow in your IDE.

## Running the example workflows within the IDE

The example workflows are under the examples module. They are in the package fire.examples.workflow.
The github project already contains the data that are used by the example workflows. The input data path is
hardcoded in the examples workflows. Hence no additional parameter needs to be specified while executing them.

Just go ahead and run/debug the example workflows within your IDE.


## Running the example workflows on a Spark Cluster

Use the command below to load example data onto HDFS from the edge node of your Hadoop Cluster. It is then used by
the example Workflows.

	hadoop fs -put data

Below are commands to run the various example Workflows on a Spark cluster.

Executor memory of 2G, 4 executors with 2G each has been specified in the commands. The parameter **'cluster'** specifies that we are running the workflow on a cluster as against locally. This greatly simplifies the development and debugging within the IDE by setting its value to **'local'** or not specifying it.

	spark-submit --class fire.examples.workflow.ml.WorkflowKMeans --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

	spark-submit --class fire.examples.workflow.ml.WorkflowLinearRegression --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

	spark-submit --class fire.examples.workflow.ml.WorkflowLogisticRegression --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

	spark-submit --class fire.examples.workflow.ml.WorkflowParquet --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

The full list of example Workflows is documented here : https://github.com/FireProjects/fire/blob/master/docs/RunningExampleWorkflowsOnHadoopCluster.md