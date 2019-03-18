package com.spark.wordcount.controller;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Component
public class sparkSqlWebSelect  {

	/**
	 * 
	 */
	

	
	
	@RequestMapping(value = "/select", method = RequestMethod.GET)
	//public static String mytestRDD( String wenjianId) {
	public  String mytestRDD(@RequestParam(name = "id", required = true) String wenjianId) {
		// TODO Auto-generated method stub
		SparkConf sc = new SparkConf()
				.setAppName("selectApp")
				//.setJars(JavaSparkContext.jarOfClass(this.getClass()))
		/*SparkConf sc = new SparkConf()
				.setAppName("selectApp")
				.setJars(JavaSparkContext.jarOfClass(this.getClass()));*/
				//.setMaster("spark://192.168.1.170:7077");
				//.setJars(null);
				//.set("spark.eventLog.enabled", "true")
				//.setJars(new String[]{"/opt/spark_test/spark_sql/spark.worldcount-0.0.1-SNAPSHOT.jar"});
				.setMaster("local[*]");
				//.setMaster("yarn-client");
		//SparkSession spark = SparkSession.builder().config(sc).getOrCreate();
		SparkSession spark = SparkSession.builder().config(sc).getOrCreate();
		String url = "jdbc:mysql://192.168.1.22:3306/database?user=etl;password=etl_xiyu123";
		String sql = "( SELECT * from `database`.CW_CAIWU_SHUJU"+" where wenjian_id= "+wenjianId+") as test_table";
		Dataset<Row> dfwhere = spark.read().format("jdbc").option("url", url).option("dbtable", sql).load();
		//dfwhere.cache();
		 dfwhere.show(10);
		//dfwhere.filter(dfwhere.col("YEMA").equalTo("128_1")).show(522);
		//dfwhere.show();
		dfwhere.foreach(line->System.out.println(line));
		// dfwhere.foreach(line->System.out.println(line));
		// dfwhere.foreachPartition(line->System.out.println(System.getProperty("java.class.path")) );
		 //dfwhere.collectAsList().forEach(line->System.out.println(line));
		/* Properties connectionProperties = new Properties();
			connectionProperties.put("user", "etl");
			connectionProperties.put("password", "etl_xiyu123");*/
			

			// rewriteBatchedStatements=true增加mysql 插入的速度增加
			//dfwhere.write().mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.1.22:3306?rewriteBatchedStatements=true","database.CW_CAIWU_SHUJU_copy", connectionProperties);
		// dfwhere.toJavaRDD().map(line->{System.out.println(line); return null;}).collect();
		 
		spark.stop();
		return "ok";
	}
	
}
