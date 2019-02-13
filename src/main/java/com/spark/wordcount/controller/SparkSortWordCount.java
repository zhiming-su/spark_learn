package com.spark.wordcount.controller;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import scala.Tuple2;

/**
 * 
 * @author suzhi
 *单词排序demo
 */
public class SparkSortWordCount {

	public static void wordCount() {
		SparkConf sc = new SparkConf();
		sc.setAppName("Word");
		sc.setMaster("local");
		
		JavaSparkContext jsc = new JavaSparkContext(sc);
		
		JavaRDD<String> lines = jsc.textFile("J://Study//Spark//hello.txt");
		
		//flatMap->mapToPair(Tuple2)->reduceByKey->mapToPair()......
		JavaPairRDD<String,Integer> fmap =	lines.flatMap(f-> {return Arrays.asList(f.split(" ")).iterator();})
				.mapToPair(word-> new Tuple2<>(word,1))
				.reduceByKey((v,k) ->  v+k).mapToPair(f -> new Tuple2<>(f._2,f._1)).sortByKey(false).mapToPair(f -> new Tuple2<>(f._2,f._1));
		//JavaPairRDD<Integer,String> fmapSort
		fmap.foreach(f -> System.out.println(f._1+" "+f._2));
		jsc.close();
	}
}
