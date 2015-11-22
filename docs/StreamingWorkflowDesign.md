# Streaming Workflow Design

Streaming Workflow implementation has not yet started. Streaming Workflow would enable building of Workflows which
get executed in Spark Streaming.

### Nodes Supported

Below is the initial list of nodes which would be supported in Streaming Workflow.

**Input Nodes**

* Kafka
* Flume
* HDFS
* S3
* Kinesis
* Twitter

**Output Nodes**

* HDFS
* HBase
* Flume
* Kafka
* Cassandra
* Kinesis
* Solr


**Transform Nodes**

* Aggregation
* Window

### Streaming Workflow

**fire.workflowenginestreaming.WorkflowStreaming** represents the spark streaming workflow.

It contains an array of dataset nodes which ingest or load data from some system like Kafka/Flume etc.

    public ArrayList<NodeStreaming> datasetNodes = new ArrayList<>();

It has an execute() method which executes the workflow.

    public void execute(JavaStreamingContext ctx, WorkflowContext workflowContext)

It also has a getSchema() method which returns the schema of a given node id.

    public NodeSchema getSchema(int nodeId)

### NodeStreaming

The class fire.workflowstreaming.NodeStreaming represents the parent node for all the Streaming Nodes.

Apart from **id** and **name** it has list of next nodes it is connected with.

    public List<NodeStreaming> nextNodes = new LinkedList<>();

It has a getSchema() method that returns the output schema of the node given its input schema. Implementing nodes
can override it.

    public NodeSchema getSchema(int nodeId, NodeSchema inputSchema)


It also has an execute() method which executes the current nodes and then calls execute() on the nodes it is connected
to. By default it just calls execute on the nodes it is connected with. Implementing nodes can override it.

    public void execute(JavaStreamingContext ctx, WorkflowContext workflowContext,
                            JavaDStream<Row> dstream, NodeSchema schema)

