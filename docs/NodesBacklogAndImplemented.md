## Nodes Backlog

If you are looking to contribute a Node, below are some ideas:

**Loading Data**

* Node for loading data into HBase
* Node for loading data into Solr



## Nodes Implemented

The following Nodes have been implemented till now. They reside under :

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

This section contains the list of Nodes that could be added in the future. More Nodes would continue to be added here.

#### HBase Nodes

* **LoadDataIntoHBase.java**

#### ETL Nodes

* **GroupBy.java**



