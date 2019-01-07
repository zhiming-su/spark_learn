package com.spark.wordcount.controller;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkActionTest {
	/**
	 * action reduce操作
	 */
	public static void retudce() {
		SparkConf scf = new SparkConf();
	    scf.setAppName("action");
	    scf.setMaster("local");
	    
	    JavaSparkContext jsc = new JavaSparkContext(scf);  
	    
	    List<Integer> number = Arrays.asList(1,2,3,4,5);
		 
	    JavaRDD<Integer> str = jsc.parallelize(number);
	    
	  Integer get =  str.reduce((f,b)->{return f+b;});
	  System.out.println(get);
	    
		jsc.close();
	}
	/**
	 * action conclect操作把集群上的rdd数据拉取到本地driver上
	 * 这种方式不建议使用，如果数据量比较大的情况会比较慢
	 * 因此推荐使用foreach方法对结果集处理
	 */
	public static void collect() {
		SparkConf scf = new SparkConf();
	    scf.setAppName("action");
	    scf.setMaster("local");
	    
	    JavaSparkContext jsc = new JavaSparkContext(scf);  
	    
	    List<Integer> number = Arrays.asList(1,2,3,4,5);
		 
	    JavaRDD<Integer> str = jsc.parallelize(number).map(f ->{return f*2;});
	    
	  //Integer get =  str.collect((f,b)->{return f+b;});
	 // System.out.println(get);
	    List<Integer> list = str.collect();
	    list.forEach(f -> System.out.println(f));
		jsc.close();
	}
	/**
	 * count统计有多少个元素
	 */
	public static void count() {
		SparkConf scf = new SparkConf();
	    scf.setAppName("action");
	    scf.setMaster("local");
	    
	    JavaSparkContext jsc = new JavaSparkContext(scf);  
	    
	    List<Integer> number = Arrays.asList(1,2,3,4,5);
		 
	    JavaRDD<Integer> str = jsc.parallelize(number);
	    
	    Long get =  str.count();
	    System.out.println(get);
		jsc.close();
	}
	/**
	 * take 与collect类似，也是同远程集群上获取rdd数据
	 * take只是获取部分数据
	 */
	public static void take() {
		SparkConf scf = new SparkConf();
	    scf.setAppName("action");
	    scf.setMaster("local");
	    
	    JavaSparkContext jsc = new JavaSparkContext(scf);  
	    
	    List<Integer> number = Arrays.asList(1,2,3,4,5);
		 
	    JavaRDD<Integer> str = jsc.parallelize(number);
	    
	   List<Integer> list = str.take(3);
	    list.forEach(f->{System.out.println(f);});
		jsc.close();
	}
	/**
	 * saveAsTextFile
	 */
	public static void saveFile() {
		SparkConf scf = new SparkConf();
	    scf.setAppName("action");
	    scf.setMaster("local");
	    
	    JavaSparkContext jsc = new JavaSparkContext(scf);  
	    
	    List<Integer> number = Arrays.asList(1,2,3,4,5);
		 
	    JavaRDD<Integer> str = jsc.parallelize(number);
	    
	    //目录结构
	    //hdfs
	  str.saveAsTextFile("hdfs://spark01:9000/sparkSaveAsTextFile");
		jsc.close();
	}
	/**
	 * countByKey 根据key统计key相同的个数
	 */
	public static void countByKey() {
		SparkConf scf = new SparkConf();
	    scf.setAppName("action");
	    scf.setMaster("local");
	    
	    JavaSparkContext jsc = new JavaSparkContext(scf);  
	    
	    List<Tuple2<String,String>> number = Arrays.asList(new Tuple2<>("class1","Tom"),new Tuple2<>("class2","Tom2"),new Tuple2<>("class3","Tom3"),new Tuple2<>("class1","Tom4"));
		 
	    JavaPairRDD<String, String> str = jsc.parallelizePairs(number);
	    
	   Map<String, Long> mp =  str.countByKey();
	   
	   mp.forEach((a,b) ->{System.out.println(a+":"+b);});
	    
		jsc.close();
	}
}
