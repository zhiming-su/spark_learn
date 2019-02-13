package com.spark.wordcount.controller;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 
 * @author suzhi
 *自定义的二次排序
 */
public class SparkSecondSort {

	public static void secondSort() {
		SparkConf sc = new SparkConf();
		sc.setAppName("SecondSort");
		sc.setMaster("local");
		
		JavaSparkContext jsc = new JavaSparkContext(sc);
		//jsc.setLogLevel("TRACE");
		
		JavaRDD<String> lines = jsc.textFile("J://Study//Spark//sort.txt");
		
		//JavaRDD<String> getNumber = 
		//		lines.map(f-> {return f;});//.mapToPair(f-> new Tuple2<>(f.split(" ")[0],f.split(" ")[1]);));
		//getNumber.foreach(f->System.out.println(f.split(" ")[0]+" "+f.split(" ")[1]));
		JavaPairRDD<SparkSecondSortKey,String> jpr= lines.mapToPair(f->{
			String[] linesStr = f.split(" ");
			SparkSecondSortKey keys = new SparkSecondSortKey(Integer.valueOf(linesStr[0]),Integer.valueOf(linesStr[1]));
			return new Tuple2<>(keys,f);
			}).sortByKey();
		jpr.foreach(f->System.out.println(f._2));
		jsc.close();
	}
}
