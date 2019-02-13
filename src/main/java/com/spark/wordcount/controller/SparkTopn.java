package com.spark.wordcount.controller;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkTopn {

	public static void topnTest() {
		SparkConf sc = new SparkConf();
		sc.setAppName("Topn");
		sc.setMaster("local");
		
		
		JavaSparkContext jsc = new JavaSparkContext(sc);
		
		JavaRDD<String> lines = jsc.textFile("J://Study//Spark//top.txt");
		
		//JavaRDD<String> getstr = lines.map(f-> f).top(3);
		List<Integer> getstr = lines.map(f-> Integer.valueOf(f)).sortBy(f->f, true, 2).top(3);
		getstr.forEach(f->System.out.println(f));
		//getstr.foreach( f -> System.out.println(f));
		jsc.close();
		
	}
}
