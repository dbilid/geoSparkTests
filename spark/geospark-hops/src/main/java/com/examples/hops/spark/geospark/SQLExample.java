package com.examples.hops.spark.geospark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator;

import com.esotericsoftware.minlog.Log;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class SQLExample {

	public static void main(String[] args) {
		// arg[0]: top directory of filesystem e.g. hdfs:///
		// arg[1]: directory of CSV files
		// arg[2]: directory of queries e.g scalability100M/
		SparkSession sparkSession = SparkSession.builder()
				// .master("local[*]")
				.appName("GeoSpark_SQLExample").config("spark.serializer", KryoSerializer.class.getName())
				.config("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName()).getOrCreate();

		GeoSparkSQLRegistrator.registerAll(sparkSession);
		Path[] files;
		String resourceDir=args[2];
		if(!resourceDir.endsWith("/")) {
			resourceDir+="/";
		}
		try {
			files = getPaths("hdfs:///", args[1]);
		} catch (Exception e) {
			System.err.println("Could not open directory given in args");
			return;
		}
		for (int i = 0; i < files.length; i++) {
			if (!files[i].getName().endsWith(".csv"))
				continue;
			System.out.println("name:" + files[i].getName());
			System.out.println("toString:" + files[i].toString());
			Dataset<Row> nextDf = sparkSession.read().format("csv").option("delimiter", ",").option("header", "true")
					.load(files[i].toString());
			if (files[i].getName().equals("public_geo_values.csv")) {
				// geometries
				nextDf.createOrReplaceTempView("tempgeo");
				Dataset<Row> spatialDf = sparkSession
						.sql("SELECT id, srid, ST_GeomFromWKT(st_astext) as  strdfgeo " + "FROM tempgeo");
				spatialDf.createOrReplaceTempView(files[i].getName().replace("public_", "").replace(".csv", ""));
				// spatialDf.show();
			} else {
				nextDf.createOrReplaceTempView(files[i].getName().replace("public_", "").replace(".csv", ""));
				// rawDf.show();
			}
		}
		// sparkSession.sql("CACHE TABLE aswkt_3");
		// sparkSession.sql("CACHE TABLE geo_values");
		// sparkSession.sql("CACHE TABLE uri_values");
		// sparkSession.sql("CACHE TABLE label_values");
		// sparkSession.sql("CACHE TABLE has_code_73");
		// sparkSession.sql("CACHE TABLE hasgeometry_79");
		long start = System.currentTimeMillis();
		String q = getResourceContents(resourceDir + "query1.q");

		Dataset<Row> query1 = sparkSession.sql(q);
		query1.createOrReplaceTempView("query1");
		// query3.show();
		long res = query1.count();
		Log.info("results: " + res);
		System.out.println("results from query 1: " + res + " in " + (System.currentTimeMillis() - start) + " ms");
		start = System.currentTimeMillis();

		q = getResourceContents(resourceDir + "temp1.q");

		Dataset<Row> temp1 = sparkSession.sql(q);
		temp1.createOrReplaceTempView("temp1");
		q = getResourceContents(resourceDir + "temp2.q");

		Dataset<Row> temp2 = sparkSession.sql(q);
		temp2.createOrReplaceTempView("temp2");
		q = getResourceContents(resourceDir + "query3.q");

		Dataset<Row> query3 = sparkSession.sql(q);

		query3.createOrReplaceTempView("query3");
		res = query3.count();
		Log.info("results: " + res);
		System.out.println("results from query 3: " + res + " in " + (System.currentTimeMillis() - start) + " ms");
		start = System.currentTimeMillis();
		q = getResourceContents(resourceDir + "temp1b.q");

		Dataset<Row> temp1b = sparkSession.sql(q);
		temp1b.createOrReplaceTempView("temp1b");

		q = getResourceContents(resourceDir + "query2.q");

		Dataset<Row> query2 = sparkSession.sql(q);

		query2.createOrReplaceTempView("query2");
		res = query2.count();
		Log.info("results: " + res);
		System.out.println("results from query 2: " + res + " in " + (System.currentTimeMillis() - start) + " ms");

	}

	private static Path[] getPaths(String fs, String path) throws IOException, URISyntaxException {
		Configuration configuration = new Configuration();
		// 2. Get the instance of the HDFS
		FileSystem hdfs = FileSystem.get(new URI(fs), configuration);
		// 3. Get the metadata of the desired directory
		FileStatus[] fileStatus = hdfs.listStatus(new Path(path));
		// 4. Using FileUtil, getting the Paths for all the FileStatus
		return FileUtil.stat2Paths(fileStatus);
	}

	public static String getResourceContents(String resource) {
		BufferedReader br = new BufferedReader(
				new InputStreamReader(ClassLoader.getSystemClassLoader().getResourceAsStream(resource)));
		StringBuilder bf = new StringBuilder();
		String line;
		try {
			line = br.readLine();
			while (line != null) {
				bf.append(line);
				line = br.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return bf.toString();

	}

}
