# Skyline Queries with Hadoop MapReduce
In the context of "Advanced Topics in Database Systems" course
Technical University of Crete

Input files available here 
```
https://www.dropbox.com/sh/wygdm7v9ex3avaf/AACNpEUHhjYQ2k0yNJ3rUbB4a?dl=0
```

To execute with jar type
```
hadoop jar JarName.jar input_file_1X.csv OutputFileName.csv NumberOfPartions random/angle Dimensions
```

Examples
```
hadoop jar Sky.jar input_file_1000.csv Skyline.csv 9 random 2
hadoop jar Sky.jar input_file_1000.csv Skyline.csv 6 random 2
hadoop jar Sky.jar input_file_1000.csv Skyline.csv 3 angle 2
hadoop jar Sky.jar input_file_10000.csv Skyline.csv 2 angle 3
```

Angle based partitioning supports only 9 way partitioning at 3 dimensions.
