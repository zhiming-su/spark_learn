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

import scala.Tuple2;

/**
 * 
 * 提供rest api
 *
 */
@Component
// @RequestMapping("/test/*")
//@RestController
public class SparkController {

	/**
	 * schedulix_api
	 * 
	 * @return
	 * @throws IOException 
	 */

	// @RequestMapping(value="/hello",method=RequestMethod.POST)
//	@RequestMapping(value = "/index", method = RequestMethod.GET)
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
		//sc.setMaster("local");
		
		//sparkContext核心控件
		JavaSparkContext jsc = new JavaSparkContext(sc);
		//jsc.textFile(path, minPartitions)
		//创建一个rdd,java中的RDD，都叫做JavaRDD，在RDD中，有元素的概念，每一个元素就相当于文件里的一行
		//JavaRDD<String> lines = jsc.textFile("J://Study//Spark//spark.txt");
		JavaRDD<String> lines = jsc.textFile("hdfs://spark01:9000/spark.txt");
		//对初始RDD进行transformatino操作，也就是计算操作
		//对每一行进行单词拆分
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			
			@Override
			public Iterator<String> call(String line) throws Exception {
				// TODO Auto-generated method stub
				return  (Iterator<String>) Arrays.asList(line.split(" "));
			}
		});
		//接着需要将每一个单词映射为（单词，1）的这种格式
		//为了累加单词出现的次数
		//tuple2为scala类型元祖
		//第一个参数，为输入类型
		//第二个，第三个为输出类型为tuple2
		//javaPariRDD 对应scala tuple2类型
		JavaPairRDD<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String,Integer>(word,1);
			}
		});
		
		//使用reduceByKey这个算子，对每个key的value都进行reduce操作
		//reduce操作，相当于把第一个和第二个值进行计算，然后再将结果与第三个值进行计算
		//最后返回的javaPairRDD中的元素，也是tuple类型，但是第一个值就是每个key,第二个值就是key的value
		//reduce之后的结果，相当于就是每个单词出现的次数
		JavaPairRDD<String,Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		});
		
		//action 操作 相当于执行
		wordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			@Override
			public void call(Tuple2<String, Integer> wordCount) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(wordCount._1+" "+wordCount._2);
			}
		});
		jsc.stop();
	}

	
}