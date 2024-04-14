# Outlier Detector Pyspark
This repository contains the implementation of the homework for the Big Data Computing course at our Master's Degree in Computer engineering at UNIPD. 

Outlier (or anomaly) detection is a fundamental task in data analysis but it is often very expensive from a computational point of view; for this reason
the program implemetents two possible approaches, an exact one and an approximate one, to identificate anomalies in a set of N points in the Euclidean space with dimensionality 2.

The purpose of this project is to compare the two algorithms, get acquainted with Spark and with its use to implement MapReduce algorithms.

To run the main program use the python script `G023HW1.py`. Such script accepts various parameters: 
```
usage: GO23HW1.py <file_name> <D> <M> <K> <L>

arguments:
- file_name : input file with point's coordinate
- D         : distance to consider around each point, outliers distance
- M         : minimum number of points inside a cell to not be an outliers
- K         : number of points to be printed
- L         : number of partitions for pyspark
```

