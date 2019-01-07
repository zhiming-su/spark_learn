package com.spark.wordcount.controller;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class SparkRDDTest {
	/**
	 * RDD持久化操作，提高对数据的使用效率
	 * 持久化缓存lines RDD的所有数据
	 * 官方文档说，合理的RDD持久化机制，甚至可以提升spark应用程序的性能10倍左右
	 * 使用cache()方法提供持久化操作
	 * JavaRDD<String> str = jsc.textFile("J://Study//Spark//hello.txt").cache(); 正确的方法
	 * str.cache();这样使用是没有作用的
	 * 或者使用persist();
	 * StorageLevel.MEMORY_AND_DISK()
	 * StorageLevel.MEMORY_ONLY()
	 * StorageLevel.MEMORY_ONLY_SER() 序列化方式，cpu使用高
	 * .MEMORY_AND_DISK_SER_2() 2表示有备份的策略，这要在失败时，就不需要重新计算了
	 */
	public static void cache() {
		SparkConf scf = new SparkConf();
	    scf.setAppName("RDD");
	    scf.setMaster("local");
	    
	    JavaSparkContext jsc = new JavaSparkContext(scf);  
	    
	    
	    JavaRDD<String> str = jsc.textFile("J://Study//Spark//hello.txt").persist(StorageLevel.MEMORY_AND_DISK_SER_2());
	   // str.cache();
	    //str.persist(1);
	    long startTime = System.currentTimeMillis();
	    Long num = str.count();
	    System.out.println(num);
	    long endTime = System.currentTimeMillis();
	    System.out.println("Times: "+(endTime-startTime));
	    
	    long startTime1 = System.currentTimeMillis();
	    Long num1 = str.count();
	    System.out.println(num1);
	    long endTime1 = System.currentTimeMillis();
	    System.out.println("Times: "+(endTime1-startTime1));
	  
		jsc.close();
	}
}
