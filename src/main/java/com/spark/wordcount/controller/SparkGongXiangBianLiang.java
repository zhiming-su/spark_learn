package com.spark.wordcount.controller;


import java.util.List;
import java.util.Arrays;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;

public class SparkGongXiangBianLiang {

	/**
	 * 共享变量
	 * 
	 * broadcast variable(广播变量),只读的
	 * accumulator(累加变量)
	 *  
	 */
	public static void gonxiangbianliang() {
		SparkConf scf = new SparkConf();
	    scf.setAppName("RDD");
	    scf.setMaster("local");
	    
	    JavaSparkContext jsc = new JavaSparkContext(scf);  
	    
	    final int b = 3;
	    //创建共享变量
	    final Broadcast<Integer>  factorBroadcast = jsc.broadcast(b);
	    List<Integer> list = Arrays.asList(1,2,3,4,5);
	    
	    JavaRDD<Integer> get = jsc.parallelize(list).map(f -> { int factor = factorBroadcast.value(); return f*factor;});
	    
	    get.foreach(f -> System.out.println(f));
	  
		jsc.close();
	}
	
	/**
	 * 共享变量
	 * 
	 * broadcast variable(广播变量),只读的
	 * accumulator(累加变量)
	 * accumulator特性，task只能累加值，不能读取其值， 只有driver能读取其值
	 *  
	 */
	public static void gonxiangbianliangAccumlator() {
		SparkConf scf = new SparkConf();
	    scf.setAppName("RDD");
	    scf.setMaster("local");
	    
	    JavaSparkContext jsc = new JavaSparkContext(scf);  
	    //过时的累加器方法
	    //jsc.intAccumulator(0);
	    
	    //v2.4版本的新用法，sc()返回的是sparkcontext()方法
	    LongAccumulator longacc=jsc.sc().longAccumulator("Number");
	  //  Accumulator<Integer> al = jsc.accumulator(0);

	    List<Integer> list = Arrays.asList(1,2,3,4,5);
	    
	    JavaRDD<Integer> get = jsc.parallelize(list);
	    
	    //get.foreach(f -> al.add(1));
	    get.foreach(f -> {longacc.add(1);});
	    try {
			Thread.sleep(100000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    System.out.println(longacc.value());
	  
		jsc.close();
	}
}
