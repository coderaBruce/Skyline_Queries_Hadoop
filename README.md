# Skyline Queries with Hadoop for Apartment Selection
In the context of "Big Data Analytics" course
Kent State University
@author Xinyu Li, Hailong Jiang

To execute with jar type
```
First upload input files to HDFS,then:

hadoop jar JarName.jar input_file_1X.csv OutputFileName.csv NumberOfPartions random/angle Dimensions
```

Examples
```
hadoop jar Sky.jar input_file_1000.csv Skyline.csv 9 random 2
hadoop jar Sky.jar input_file_1000.csv Skyline.csv 6 random 2
hadoop jar Sky.jar input_file_1000.csv Skyline.csv 3 angle 2
hadoop jar Sky.jar input_file_10000.csv Skyline.csv 9 angle 3
hadoop jar Sky.jar FullPath\input_file_1000.csv FullPath\Skyline.csv 9 random 3
```

Angle based partitioning supports only 9 way partitioning at 3 dimensions.
