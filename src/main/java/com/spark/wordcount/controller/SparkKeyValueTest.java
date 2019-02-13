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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import scala.Function;
import scala.Tuple2;


/**
 * 
 * 统计每行出现的次数
 *
 */
@Component
// @RequestMapping("/test/*")
@RestController
public class SparkKeyValueTest {

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
		JavaRDD<String> lines = jsc.textFile("J://Study//Spark//hello.txt");
		
		JavaPairRDD<String, Integer> counts = lines
			    .mapToPair(word -> new Tuple2<>(word, 1))
			    .reduceByKey((a, b) -> a + b);
			//counts.saveAsTextFile("hdfs://...");
			//action 操作 相当于执行
		
			counts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
				
				@Override
				public void call(Tuple2<String, Integer> wordCount) throws Exception {
					// TODO Auto-generated method stub
					System.out.println(wordCount._1+" "+wordCount._2);
				}
			});
			
			
		
		jsc.stop();
	}

	
}