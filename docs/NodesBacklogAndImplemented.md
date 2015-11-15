## Nodes Backlog

If you are looking to contribute a Node, below are some ideas:

**HBase**

* Node for loading data into HBase

**Solr**

* Node for loading data into Solr

**NLP Nodes**

It would be great to have NLP integrated into the system. We plan to use OpenNLP.

**OCR Nodes**

Optical Character Recognition is useful and we plan to integrate it into the system.

**Omniture Nodes**

* Node to clean omniture data
* Node to generate analytics from omniture data



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





