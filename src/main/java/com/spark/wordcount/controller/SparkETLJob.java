package com.spark.wordcount.controller;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkETLJob {

	public static void etlJob() throws InterruptedException, ExecutionException {
		SparkConf conf = new SparkConf().setAppName("ETL_JOB").setMaster("local");

		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		CompletableFuture<String> future2 = CompletableFuture.supplyAsync(() -> get1(spark));
		CompletableFuture<String> future1 = CompletableFuture.supplyAsync(() -> get(spark));

		System.out.println(future2.get());
		System.out.println(future1.get());
		spark.close();
	}

	public static String get(SparkSession spark) {
		Dataset<Row> jrdd = spark.read().json("J://Study//Spark//students.json");
		
		jrdd.show();
		return "1";
	}

	public static String get1(SparkSession spark) {
		Dataset<Row> jrdd = spark.read().json("J://Study//Spark//students.json");
		jrdd.show();
		return "2";
	}
}
