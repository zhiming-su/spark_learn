package com.spark.wordcount.controller;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;



public class SparkGroupTopn {

	public static void groupTopn() {
		SparkConf sc = new SparkConf().setAppName("GroupTopn").setMaster("local");
		
		JavaSparkContext jsc = new JavaSparkContext(sc);
		
		JavaRDD<String> lines = jsc.textFile("J://Study//Spark//score.txt");
		
		JavaPairRDD<String, Iterable<Integer>> getStr = lines.mapToPair(f-> new Tuple2<>(f.split(" ")[0],Integer.valueOf(f.split(" ")[1])))
				.groupByKey();
		
		
		getStr.foreach(f->
		{
			//System.out.println(f._1);
			//List<Iterable<Integer>> it = Arrays.asList(f._2);
			//System.out.println(it.iterator().next());
			List<Integer> it1 = new ArrayList();
			f._2.forEach(f1 ->{
				it1.add(f1);
			 }
			);
			Collections.sort(it1);
			Collections.reverse(it1);
			Iterator<Integer> it =  it1.iterator();
			int i=1;
			while(it.hasNext()) {
				
				if(i<4) {
					System.out.println(f._1+" "+it.next());
				}else {
					break;
				}
				i++;
			}
			
			//num1.forEach(f2 -> System.out.println(f2));
		});
		//getStr.forEach(f->System.out.println(f));
		
		jsc.close();
	}
	
}
		
		