package com.spark.wordcount.controller;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.springframework.stereotype.Component;

import scala.Function;
import scala.Tuple2;


/**
 * 
 * 提供rest api
 *
 */
@Component
// @RequestMapping("/test/*")
//@RestController
public class SparkMapLocal {

	/**
	 * schedulix_api
	 * 
	 * @return
	 * @throws IOException 
	 */

	// @RequestMapping(value="/hello",method=RequestMethod.POST)
	//@RequestMapping(value = "/index", method = RequestMethod.GET)
	/*
	 * public String
	 * bankStatement(@RequestParam(name="projectId",required=true)String
	 * projectId,@RequestParam(name="fileId",required=true)String fileId) { return
	 * "a"; }
	 */
	public String submit() throws IOException {
		sparkConf();
	
			return "OK";
		}
		// return "Hello World";

	

	public static void sparkConf() throws IOException {
		//Resource rc = new ClassPathResource("/opt/spark.txt");
		//sparkconf 配置
		//提交到集群要把setMaster的方法删掉
		SparkConf  sc = new SparkConf();
		sc.setAppName("WordCount");
		sc.setMaster("local");
		
		//sparkContext核心控件
		JavaSparkContext jsc = new JavaSparkContext(sc);
		//jsc.textFile(path, minPartitions)
		//创建一个rdd,java中的RDD，都叫做JavaRDD，在RDD中，有元素的概念，每一个元素就相当于文件里的一行
		JavaRDD<String> lines = jsc.textFile("J://Study//Spark//spark.txt");
		
		//JavaRDD<String> textFile = sc.textFile("hdfs://...");
		//统计字数
		JavaRDD<Integer> counts = lines.map(str -> str.length());
		Integer num = counts.reduce((a,b) ->a+b); 
		//counts.saveAsTextFile("hdfs://...");
		//action 操作 相当于执行
		System.out.println(num);
		
		jsc.stop();
	}

	
}