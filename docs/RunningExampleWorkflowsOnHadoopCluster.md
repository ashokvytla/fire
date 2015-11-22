# Running Example Workflows on Hadoop/Spark Cluster

The example workflow can be run within an IDE in local mode or on the Hadoop/Spark cluster. This document covers
running the example workflows on the Hadoop/Spark cluster.

## Check out code from github

	git clone https://github.com/FireProjects/fire.git

## Build it with maven

Ensure that you have Java and Maven set up. Build Fire with:

	mvn package


## Load example data into HDFS

Use the command below to load example data in fire onto HDFS. These are then used by the example Workflows.

	hadoop fs -put data


## Running the batch workflows

Below are commands to run the various batch example Workflows on a Spark cluster.

Executor memory of 2G, 4 executors with 2G each has been specified in the commands. The parameter **'cluster'**
specifies that we are running the workflow on a cluster as against locally. This greatly simplifies the development
and debugging within the IDE by setting its value to **'local'** or not specifying it.

**KMeans**

	spark-submit --class fire.examples.workflow.ml.WorkflowKMeans --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

**Linear Regression**

	spark-submit --class fire.examples.workflow.ml.WorkflowLinearRegression --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

**Logistic Regression**

	spark-submit --class fire.examples.workflow.ml.WorkflowLogisticRegression --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

**Decision Tree**

	spark-submit --class fire.examples.workflow.ml.WorkflowDecisionTree --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

**ALS**

	spark-submit --class fire.examples.workflow.ml.WorkflowALS --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

**Reading Parquet File**

	spark-submit --class fire.examples.workflow.ml.WorkflowParquet --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

**Logistic Regression on Housing Data**

	spark-submit --class fire.examples.workflow.ml.WorkflowHousing --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

**Joining 2 CSV files on a column**

	spark-submit --class fire.examples.workflow.ml.WorkflowJoin --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

**KMeans workflow from a workflow JSON file**

	spark-submit --class fire.examples.workflow.ml.WorkflowFromFile --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

## Running the streaming workflows

Below are the commands to run the various streaming example Workflows on a Spark cluster

**Streaming word count**

    Run Netcat as a data server : nc -lk 9999
	spark-submit --class fire.examples.workflowstreaming.WorkflowSocket --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

