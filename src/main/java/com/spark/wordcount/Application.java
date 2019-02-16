package com.spark.wordcount;

import java.io.IOException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.spark.wordcount.controller.SparkSqlHive;

@SpringBootApplication
public class Application {

	public static void main(String[] args) throws IOException {
		SpringApplication.run(Application.class, args);
		//SparkControllerLocal.sparkConf();
		//SparkController24.sparkConf();
		//SparkSql.dataFrameFunJson();
		//SparkSql.RddToDataFrame2();
		//SparkSql.dataFrameFunJson();
		//SparkSql.saveData();
		//SparkSql.readParquet();
		//SparkSql.jsonJoin();
		SparkSqlHive.hiveSql();
		//SparkSecondSort.secondSort();
	}
}
