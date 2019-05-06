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

## Bare geoSpark application

* Move to geospark-bare project
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
/home/kgiann/Downloads/geospark-examples-spark-bare-1.0.0-SNAPSHOT-jar-with-dependencies.jar num_of_partitions num_of_cores
```

## See results

In the [Projects/demo_spark_kgiann01/Resources](http://localhost:50070/explorer.html#/Projects/demo_spark_kgiann01/Resources) HDFS directory you will find another file named geospatial_results.txt. Every run of the spark application will write there some interesting information. i.e.

```text
System, Predicate, Indexed, Partitions, Cores, Warmup, Avg Execution Time, ResultSet Count
GeoSpark, PolygonsContainPolygons, false, 1, 8, 10.943, 8.366999999999999, 5959
GeoSpark, PolygonsContainPolygons, false, 2, 16, 10.897, 8.689333333333332, 5959
```

Notice the number of partitions and cores changing as you pass them when running the application (num_of_partitions, num_of_cores)
