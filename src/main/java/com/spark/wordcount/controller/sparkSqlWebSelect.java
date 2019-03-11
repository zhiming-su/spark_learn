package com.spark.wordcount.controller;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class sparkSqlWebSelect {

	//@RequestMapping(value = "/select", method = RequestMethod.GET)
	public static String mytestRDD(@RequestParam(name = "id", required = true) String wenjianId) {
		// TODO Auto-generated method stub
		SparkConf sc = new SparkConf()
				.setAppName("selectApp");
				//.setMaster("local[1]");
				//.setMaster("yarn-cluster");
		SparkSession spark = SparkSession.builder().getOrCreate();
		String url = "jdbc:mysql://192.168.1.22:3306/database?user=etl;password=etl_xiyu123";
		String sql = "( SELECT * from `database`.CW_CAIWU_SHUJU"+" where wenjian_id= "+wenjianId+") as test_table";
		Dataset<Row> dfwhere = spark.read().format("jdbc").option("url", url).option("dbtable", sql).load();
		//dfwhere.cache();
		 dfwhere.show(10);
		//dfwhere.filter(dfwhere.col("YEMA").equalTo("128_1")).show(522);
		//dfwhere.show();
		
		spark.stop();
		return "ok";
	}

}
