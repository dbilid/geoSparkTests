package com.examples.hops.spark.geospark;

import java.io.IOException;

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
		SparkSession sparkSession = SparkSession.builder()
				// .master("local[*]")
				.appName("GeoSpark_SQLExample").config("spark.serializer", KryoSerializer.class.getName())
				.config("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName()).getOrCreate();

		GeoSparkSQLRegistrator.registerAll(sparkSession);
		Path[] files;
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
		long start=System.currentTimeMillis();
		Dataset<Row> query1 = sparkSession.sql("SELECT a0.obj,\n" + "         l_o1.strdfgeo, l_o1.srid,\n"
				+ "         a0.subj,\n" + "         u_s1.value\n" + "        FROM aswkt_3 a0\n"
				+ "         INNER JOIN geo_values l_o1 ON (l_o1.id = a0.obj)\n"
				+ "         INNER JOIN uri_values u_s1 ON (u_s1.id = a0.subj)\n"
				+ "        WHERE (ST_Intersects(l_o1.strdfgeo, ST_GeomFromWKT(\"POLYGON ((23.708496093749996 37.95719224376526, 22.906494140625 40.659805938378526, 11.524658203125002 48.16425348854739, -0.1181030273437499 51.49506473014367, -3.2189941406250004 55.92766341247031, -5.940856933593749 54.59116279530599, -3.1668090820312504 51.47967237816337, 23.708496093749996 37.95719224376526))\")))");

		query1.createOrReplaceTempView("query1");
		// query3.show();
		long res = query1.count();
		Log.info("results: " + res);
		System.out.println("results from query 1: " + res + " in " +(System.currentTimeMillis()-start) + " ms");
		start=System.currentTimeMillis();

		Dataset<Row> temp1 = sparkSession.sql("SELECT  " + "     h0.subj as h0subj, l_o2.strdfgeo as l2geo  \n"
				+ "    FROM hasgeometry_79 h0\n" + "     INNER JOIN aswkt_3 a1 ON (a1.subj = h0.obj)\n"
				+ "     INNER JOIN geo_values l_o2 ON (l_o2.id = a1.obj)\n"
				+ "     INNER JOIN has_code_73 h6 ON (h6.subj = h0.subj)\n"
				+ "     INNER JOIN label_values l_code2 ON (l_code2.id = h6.obj) \n"
				+ "    WHERE ( l_code2.value IN ('5622', '5601', '5641', '5621', '5661') )");
		temp1.createOrReplaceTempView("temp1");

		Dataset<Row> temp2 = sparkSession.sql(" SELECT h4.subj as h4subj, l_o1.strdfgeo as l1geo FROM \n "
				+ "     geo_values l_o1  \n" + "     INNER JOIN aswkt_3 a3 ON (a3.obj = l_o1.id)\n"
				+ "     INNER JOIN hasgeometry_79 h4 ON (h4.obj = a3.subj)\n"
				+ "     INNER JOIN has_code_73 h5 ON (h5.obj =  '1879048216'\n" + "     AND h5.subj = h4.subj) ");
		temp2.createOrReplaceTempView("temp2");

		Dataset<Row> query3 = sparkSession
				.sql(" SELECT \n " + " temp2.h4subj,  u_s1.value,   temp1.h0subj,  u_s2.value \n"
						+ "FROM temp1 INNER JOIN temp2 on ((ST_Intersects(temp1.l2geo,temp2.l1geo))) \n" +

						"     INNER JOIN uri_values u_s1 ON (u_s1.id = temp2.h4subj) \n"
						+ "     INNER JOIN uri_values u_s2 ON (u_s2.id = temp1.h0subj) ");

		query3.createOrReplaceTempView("query3");
		res = query3.count();
		Log.info("results: " + res);
		System.out.println("results from query 3: " + res + " in " +(System.currentTimeMillis()-start) + " ms");
		start=System.currentTimeMillis();

		Dataset<Row> temp1b = sparkSession.sql("SELECT  " + "     h0.subj as h0subj, l_o2.strdfgeo as l2geo  \n"
				+ "    FROM hasgeometry_79 h0\n" + "     INNER JOIN aswkt_3 a1 ON (a1.subj = h0.obj)\n"
				+ "     INNER JOIN geo_values l_o2 ON (l_o2.id = a1.obj)\n"
				+ "     INNER JOIN has_code_73 h6 ON (h6.subj = h0.subj)\n"
				+ "     INNER JOIN label_values l_code2 ON (l_code2.id = h6.obj) \n"
				+ "    WHERE ( l_code2.value > 5000 AND l_code2.value < 6000 AND l_code2.value != 5260 )");
		temp1b.createOrReplaceTempView("temp1b");

		Dataset<Row> query2 = sparkSession
				.sql(" SELECT \n " + " temp2.h4subj,  u_s1.value,   temp1b.h0subj,  u_s2.value \n"
						+ "FROM temp1b INNER JOIN temp2 on ((ST_Intersects(temp1b.l2geo,temp2.l1geo))) \n" +

						"     INNER JOIN uri_values u_s1 ON (u_s1.id = temp2.h4subj) \n"
						+ "     INNER JOIN uri_values u_s2 ON (u_s2.id = temp1b.h0subj) ");

		query2.createOrReplaceTempView("query2");
		res = query2.count();
		Log.info("results: " + res);
		System.out.println("results from query 2: " + res + " in " +(System.currentTimeMillis()-start) + " ms");
		

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

}
