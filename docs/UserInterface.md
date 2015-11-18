# Fire User Interface

The User Interface would broadly support the following:

* Creation of workflows with a drag and drop interface. Node palettes are on the LHS.
* Editing of the nodes parameters through a dialog box.
* Schema propagation is supported when editing the nodes parameters. For example when selecting the target variables,
the user is displayed a list of column names to choose from

* Serialization of the workflow into JSON and saving it on the server in an RDBMS.
* Loading of a workflow from JSON into the working area.

* Execution of a workflow on the server.
* Display of logs and results (graphs etc.) produced from the execution of the workflow on the server in the User Interface.


Nodes in the User Interface are pluggable. 

* A text file/string provides the list of Nodes supported.
* It also provides the Java classes that support each of the Nodes.
* The Java classes provide the parameters or the elements that would be displayed and edited in the specific Node Dialog box.

## UI toolkits

jsplumb would be great for building the workflows.

https://jsplumbtoolkit.com/

<img src="https://github.com/FireProjects/fire/blob/master/docs/images/Workflow.png"/>

The Workflow page would look something like below.

* Double clicking on the Split Dataset Node would bring up the Split Dataset Dialog box.
* Double clicking on the Logistic Regression Node would bring up the Logistic Regression Dialog box.

<img src="https://github.com/FireProjects/fire/blob/master/docs/images/WorkflowCompleteUI.png"/>


## Node Types

The would be different kinds of Nodes. Each Node would provide the kind of node it is. It could be:

* **Dataset Node** : Does not take input from any other node. Produces a dataset/schema as output.
* **Transform Node** : Takes input from another node and can be connected to another Transform Node.
* **Modeling Node** : Takes input from a Dataset/Transform node. Produces Model as output and can be connected to a Scoring nodes to which it sends over the Model.
* **Scoring Node** : Takes 2 inputs - Data from a Dataset/Transform node and model from a Modeling Node.
* **Join Node** : Takes multiple datasets as input from Dataset/Transform nodes and produces data at its output.
* **Split Node** : Takes data input from a Dataset/Transform Node and splits the incoming data. On it output, it can be connected to another Transform/Modeling/Scoring Node.


