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


## UI toolkits

jsplumb would be great for building the workflows.

https://jsplumbtoolkit.com/

<img src="https://github.com/FireProjects/fire/blob/master/docs/Workflow.png"/>

The Workflow page would look something like below:

<img src="https://github.com/FireProjects/fire/blob/master/docs/WorkflowCompleteUI.png"/>

