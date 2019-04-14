# GeoSpark examples


## Copy dataset to HDFS

In order to make the datasets available to the spark app, copy them into the HDFS by using the hdfs dfs commands. 
From there you can link them to the aplication either directly from the java/scala class or by setting some env variable

To add the files into the HDFS:

* Create directory:

    $hdfs dfs -mkdir -p /Projects/demo_spark_kgiann01/Resources
    $hdfs dfs -put path_to_dataset/dataset.tsv /Projects/demo_spark_kgiann01/Resources

* See the newly added files in [Projects/demo_spark_kgiann01/Resources](http://localhost:50070/explorer.html#/Projects/demo_spark_kgiann01/Resources)
