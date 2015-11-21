# Running Example Workflows

The example workflow can be run within an IDE in local mode or on the cluster

## Running the example workflows on a Spark Cluster

### Load example data into HDFS

Use the command below to load example data onto HDFS. These are then used by the example Workflows.

	hadoop fs -put data

### Running the batch workflows

Below are commands to run the various batch example Workflows on a Spark cluster.

Executor memory of 2G, 4 executors with 2G each has been specified in the commands. The parameter **'cluster'** specifies that we are running the workflow on a cluster as against locally. This greatly simplifies the development and debugging within the IDE by setting its value to **'local'** or not specifying it.

	spark-submit --class fire.examples.workflow.ml.WorkflowKMeans --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

	spark-submit --class fire.examples.workflow.ml.WorkflowLinearRegression --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

	spark-submit --class fire.examples.workflow.ml.WorkflowLogisticRegression --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

	spark-submit --class fire.examples.workflow.ml.WorkflowParquet --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster


### Running the streaming workflows

Below are the commands to run the various streaming example Workflows on a Spark cluster

    Run Netcat as a data server : nc -lk 9999
	spark-submit --class fire.examples.workflowstreaming.WorkflowSocket --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

