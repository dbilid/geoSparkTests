# GeoSpark examples


## Copy dataset to HDFS

In order to make the datasets available to the spark app, copy them into the HDFS by using the hdfs dfs commands. 
From there you can link them to the aplication either directly from the java/scala class or by setting some env variable

* Create directory and add datasets in HDFS:

```bash
$hdfs dfs -mkdir -p /Projects/demo_spark_kgiann01/Resources
$hdfs dfs -put path_to_dataset/dataset.tsv /Projects/demo_spark_kgiann01/Resources
```

* See the newly added files in [Projects/demo_spark_kgiann01/Resources](http://localhost:50070/explorer.html#/Projects/demo_spark_kgiann01/Resources)
* Build the application like any other maven app:

```bash
$mvn clean package
```

* Move the jar file to the desired location (if needed)
* Use the following command to run the spark application by saying to use the just created geospark jar application:

```bash
$spark-submit \
--class com.examples.bare.spark.geospark.PolygonsPolygons \
--master spark://pyravlos3:7077 \    
--deploy-mode client  \    
--conf spark.executor.memory=5g \    
--conf spark.driver.memory=4g \    
/home/kgiann/Downloads/geospark-examples-spark-bare-1.0.0-SNAPSHOT-jar-with-dependencies.jar
```
