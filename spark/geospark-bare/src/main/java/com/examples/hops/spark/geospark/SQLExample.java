package com.examples.hops.spark.geospark;

import java.io.IOException;

import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator;
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;


public class SQLExample {

	public static void main(String[] args) {
		//arg[0]: top directory of filesystem e.g. hdfs:///
		//arg[1]: directory of CSV files
		SparkSession sparkSession = SparkSession
                .builder()
                //.master("local[*]")
                .appName("GeoSpark_SQLExample")
                .config("spark.serializer", KryoSerializer.class.getName())
                .config("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName())
                .getOrCreate();
		
		GeoSparkSQLRegistrator.registerAll(sparkSession);
		Path[] files;
		try {
			files=getPaths(args[0], args[1]);
		} catch (Exception e) {
			System.err.println("Could not open directory given in args");
			return;
		}
		for(int i=0;i<files.length;i++) {
			if(!files[i].getName().endsWith(".csv")) continue;
			System.out.println("name:"+files[i].getName());
			System.out.println("toString:"+files[i].toString());
			Dataset<Row> nextDf=sparkSession.read().format("csv").option("delimiter", ",").option("header", "true").load(files[i].toString());
			if(files[i].getName().equals("public_geo_values.csv")) {
				//geometries
				nextDf.createOrReplaceTempView("tempgeo");
				Dataset<Row> spatialDf=sparkSession.sql("SELECT id, srid, ST_GeomFromWKT(st_astext) as  strdfgeo "
						+ "FROM tempgeo");
				spatialDf.createOrReplaceTempView(files[i].getName().replace("public_", "").replace(".csv", ""));
				//spatialDf.show();
			}
			else {
				nextDf.createOrReplaceTempView(files[i].getName().replace("public_", "").replace(".csv", ""));
				//rawDf.show();
			}
		}
		
		
		
	
		
	/*	Dataset<Row> query2 = sparkSession.sql("SELECT h4.subj,\n" + 
				"     u_s1.value,\n" + 
				"     h0.subj,\n" + 
				"     u_s2.value\n" + 
				"    FROM hasgeometry_78 h0\n" + 
				"     INNER JOIN aswkt_2 a1 ON (a1.subj = h0.obj)\n" + 
				"     INNER JOIN geo_values l_o2 ON (l_o2.id = a1.obj)\n" + 
				"     INNER JOIN geo_values l_o1 ON ((ST_Intersects(l_o1.strdfgeo,l_o2.strdfgeo)))\n" + 
				"     INNER JOIN aswkt_2 a3 ON (a3.obj = l_o1.id)\n" + 
				"     INNER JOIN hasgeometry_78 h4 ON (h4.obj = a3.subj)\n" + 
				"     INNER JOIN has_code_72 h5 ON (h5.obj =  '1879048216'\n" + 
				"     AND h5.subj = h4.subj)\n" + 
				"     INNER JOIN has_code_72 h6 ON (h6.subj = h0.subj)\n" + 
				"     INNER JOIN label_values l_code2 ON (l_code2.id = h6.obj)\n" + 
				"     INNER JOIN uri_values u_s1 ON (u_s1.id = h4.subj)\n" + 
				"     INNER JOIN uri_values u_s2 ON (u_s2.id = h0.subj)\n" + 
				"    WHERE ( l_code2.value IN ('5622', '5601', '5641', '5621', '5661') )");
		query2.createOrReplaceTempView("query2");
		query2.show();
		*/
		Dataset<Row> temp1 = sparkSession.sql("SELECT  "+
				"     h0.subj as h0subj, l_o2.strdfgeo as l2geo  \n" + 
				"    FROM hasgeometry_78 h0\n" + 
				"     INNER JOIN aswkt_2 a1 ON (a1.subj = h0.obj)\n" + 
				"     INNER JOIN geo_values l_o2 ON (l_o2.id = a1.obj)\n" + 
				"     INNER JOIN has_code_72 h6 ON (h6.subj = h0.subj)\n" + 
				"     INNER JOIN label_values l_code2 ON (l_code2.id = h6.obj) \n" + 
				"    WHERE ( l_code2.value IN ('5622', '5601', '5641', '5621', '5661') )");
		temp1.createOrReplaceTempView("temp1");
				
		Dataset<Row> temp2 = sparkSession.sql(" SELECT h4.subj as h4subj, l_o1.strdfgeo as l1geo FROM \n " + 
				"     geo_values l_o1  \n"+ 
				"     INNER JOIN aswkt_2 a3 ON (a3.obj = l_o1.id)\n" + 
				"     INNER JOIN hasgeometry_78 h4 ON (h4.obj = a3.subj)\n" + 
				"     INNER JOIN has_code_72 h5 ON (h5.obj =  '1879048216'\n" + 
				"     AND h5.subj = h4.subj) ");
		temp2.createOrReplaceTempView("temp2");
		
		Dataset<Row> result = sparkSession.sql(" SELECT \n "+
		" temp2.h4subj,  u_s1.value,   temp1.h0subj,  u_s2.value \n" + 
				"FROM temp1 INNER JOIN temp2 on ((ST_Intersects(temp1.l2geo,temp2.l1geo))) \n" +
				
				"     INNER JOIN uri_values u_s1 ON (u_s1.id = temp2.h4subj) \n" + 
				"     INNER JOIN uri_values u_s2 ON (u_s2.id = temp1.h0subj) ") ;
		
				
		result.createOrReplaceTempView("result");
		result.show();
		
		
       

	}
	
	private static Path[] getPaths(String fs, String path) throws IOException, URISyntaxException{
		Configuration configuration = new Configuration();
	    //2. Get the instance of the HDFS
	    FileSystem hdfs = FileSystem.get(new URI(fs), configuration);
	    //3. Get the metadata of the desired directory
	    FileStatus[] fileStatus = hdfs.listStatus(new Path(path));
	    //4. Using FileUtil, getting the Paths for all the FileStatus
	    return FileUtil.stat2Paths(fileStatus);
	}
	
	


}
