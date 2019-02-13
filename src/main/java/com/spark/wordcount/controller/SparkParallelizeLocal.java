package com.spark.wordcount.controller;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;


/**
 * 
 * 提供rest api
 *
 */
@Component
// @RequestMapping("/test/*")
@RestController
public class SparkParallelizeLocal {

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
		//JavaRDD<String> lines = jsc.textFile("J://Study//Spark//spark.txt");
		
		//创建并行化集合的RDD
		List<Integer> number = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> count = jsc.parallelize(number); 
		//Integer getcount=count.reduce((a,b) -> a+b);
		Integer getcount=count.reduce(new Function2<Integer,Integer,Integer>(){

			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				// TODO Auto-generated method stub
				return a+b;
			}
			
		});
		//JavaRDD<String> textFile = sc.textFile("hdfs://...");
/*		JavaPairRDD<String, Integer> counts = lines
		    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
		    .mapToPair(word -> new Tuple2<>(word, 1))
		    .reduceByKey((a, b) -> a + b);*/
		//counts.saveAsTextFile("hdfs://...");
		//action 操作 相当于执行
		/*counts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			@Override
			public void call(Tuple2<String, Integer> wordCount) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(wordCount._1+" "+wordCount._2);
			}
		});*/
		System.out.println(getcount);
		jsc.stop();
	}

	
}