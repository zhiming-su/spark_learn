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

import scala.Serializable;
import scala.Tuple2;


/**
 * 
 * 提供rest api
 *
 */
@Component
//@RequestMapping("/test/*")
//@RestController
public class SparkController24 implements Serializable {

	/**
	 * schedulix_api
	 * 
	 * @return
	 * @throws IOException 
	 */
	private final static long serialVersionUID = -3319354077527132831L;
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

	
	//transient static SparkConf sc;//定义配置信息对象
	//transient static JavaSparkContext jsc ;//声明spark上下文

	public  void sparkConf() throws IOException {
		//Resource rc = new ClassPathResource("/opt/spark.txt");
		//sparkconf 配置
		//提交到集群要把setMaster的方法删掉
		SparkConf  sc = new SparkConf();
		//sc = new SparkConf();
		sc.setAppName("WordCount");
		sc.setJars(JavaSparkContext.jarOfClass(this.getClass()));
		//sc.setJars(new String[]{"/opt/spark_test/spark_sql/spark.worldcount-0.0.1-SNAPSHOT.jar"});
		//sc.setMaster("local");
		
		//sparkContext核心控件
		JavaSparkContext jsc = new JavaSparkContext(sc);
		//jsc = new JavaSparkContext(sc);
		//jsc.textFile(path, minPartitions)
		//创建一个rdd,java中的RDD，都叫做JavaRDD，在RDD中，有元素的概念，每一个元素就相当于文件里的一行
		JavaRDD<String> lines = jsc.textFile("hdfs://spark01:9000/spark.txt");
		
		//JavaRDD<String> textFile = sc.textFile("hdfs://...");
		JavaPairRDD<String, Integer> counts = lines
		    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())//根据空格拆分为一个list
		    .mapToPair(word -> new Tuple2<>(word, 1))//把list拆分为一个（a,1）的map
		    .reduceByKey((a, b) -> a + b);//把相同的相加 
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