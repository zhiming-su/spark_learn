package com.spark.wordcount.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * 演示闭包的问题
 * @author suzhi
 *同时也的注意local模式的使用，要不然会导致出现问题
 *闭包会把一个遍历复制一个副本到一个work上去，或者是excuter上
 */
public class sparkBiBao {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//local[*]	Run Spark locally with as many worker threads as logical cores on your machine.
		SparkConf sc = new SparkConf().setAppName("etlBiBao").setMaster("local[*]");
		SparkSession spark =  SparkSession.builder().config(sc).getOrCreate();
		
		List<Integer> list = new ArrayList<Integer>();
		List<Integer> list1 = Arrays.asList(1,2,3,4,5,6);
		list.add(0);
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		JavaRDD<Integer> jrdd1 = jsc.parallelize(list1);
		//1,表示int numPartitions的数量，这样全部数据就会到一个task上面，就会只有一个副本了，所以结果是正常的
		JavaRDD<Integer> jrdd = jsc.parallelize(list1).sortBy(line->line, true, 1);
		jrdd1.foreach(line->{
			int a = list.get(0);
			a+=line;
			list.set(0, a);
			System.out.println("value "+ list.get(0)+" "+ line+" "+ a);
		});
		System.out.println("==================== ");
		jrdd.foreach(line->{
			int a = list.get(0);
			a+=line;
			list.set(0, a);
			System.out.println("value "+ list.get(0)+" "+ line+" "+ a);
			//System.out.println( line);
		});
		
		/**
		 * value 2 2 2
value 5 3 5
value 5 5 5
value 11 6 11
value 4 4 4
value 1 1 1
==================== 
value 1 1 1
value 3 2 3
value 6 3 6
value 10 4 10
value 15 5 15
value 21 6 21
value 0
		 */
		System.out.println("value "+ list.get(0));
	//	spark.close();
		jsc.close();
	}
    
}
