package com.spark.wordcount.controller;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class sparkSqlRDD {

	public static void mytestRDD() {
		// TODO Auto-generated method stub
		SparkConf sc = new SparkConf()
				.setAppName("groupBy")
				.setMaster("local");
		
		SparkSession spark = SparkSession.builder().config(sc).getOrCreate();
		String url = "jdbc:mysql://192.168.1.22:3306/database?user=etl;password=etl_xiyu123";
		String sql = "( SELECT * from `database`.ER_CW_CAIWU_SHUJU_bak20190129 ) as test_table";
		Dataset<Row> dfwhere = spark.read().format("jdbc").option("url", url).option("dbtable", sql).load();
		//dfwhere.cache();
		
		dfwhere.filter(dfwhere.col("YEMA").equalTo("128_1")).show(522);
		//dfwhere.show();
		
		spark.stop();
		
	}

}
