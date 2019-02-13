package com.spark.wordcount.controller;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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
public class SparkTransformationTest {

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
		List<Integer> number = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		//JavaRDD<String> lines = jsc.textFile("J://Study//Spark//hello.txt");
		JavaRDD<Integer> lines = jsc.parallelize(number);
		
		//Integer it = lines.reduce((a,b) -> a*2);
		JavaRDD<Integer> number1 = lines.map(a -> {return a*2;});
		
		number1.foreach(a ->{System.out.println(a);});
	    
			
		 
		jsc.stop();
	}

	/**
	 * filter过滤集合数据
	 */
	public static void filter() {
		SparkConf  sc = new SparkConf();
		sc.setAppName("WordCount");
		sc.setMaster("local");
		
		//sparkContext核心控件
		JavaSparkContext jsc = new JavaSparkContext(sc);
		//jsc.textFile(path, minPartitions)
		//创建一个rdd,java中的RDD，都叫做JavaRDD，在RDD中，有元素的概念，每一个元素就相当于文件里的一行
		List<Integer> number = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		//JavaRDD<String> lines = jsc.textFile("J://Study//Spark//hello.txt");
		JavaRDD<Integer> lines = jsc.parallelize(number);
		
		//Integer it = lines.reduce((a,b) -> a*2);
		//JavaRDD<Integer> number1 = lines.map(a -> {return a*2;});
		JavaRDD<Integer> numberRDD = lines.filter(num ->{return num%2==0;});
		
		numberRDD.foreach(a ->{System.out.println(a);});
	    
			
		 
		jsc.stop();
	}
	
	
	/**
	 * flatmap 将行及数据拆分多个
	 */
	public static void flatmap() {
		SparkConf  sc = new SparkConf();
		sc.setAppName("WordCount");
		sc.setMaster("local");
		
		//sparkContext核心控件
		JavaSparkContext jsc = new JavaSparkContext(sc);
		//jsc.textFile(path, minPartitions)
		//创建一个rdd,java中的RDD，都叫做JavaRDD，在RDD中，有元素的概念，每一个元素就相当于文件里的一行
		List<String> number = Arrays.asList("hello java","hello scala","hello perl");
		//JavaRDD<String> lines = jsc.textFile("J://Study//Spark//hello.txt");
		JavaRDD<String> lines = jsc.parallelize(number);
		
		//Integer it = lines.reduce((a,b) -> a*2);
		//JavaRDD<Integer> number1 = lines.map(a -> {return a*2;});
		JavaRDD<String> numberRDD = lines.flatMap(a -> Arrays.asList(a.split(" ")).iterator());
		
		numberRDD.foreach(a ->{System.out.println(a);});
	    
			
		 
		jsc.close();
	}
	
	/**
	 * groupByKey 分组
	 */
	public static void groupBykey() {
		SparkConf  sc = new SparkConf();
		sc.setAppName("WordCount");
		sc.setMaster("local");
		
		//sparkContext核心控件
		JavaSparkContext jsc = new JavaSparkContext(sc);
		//jsc.textFile(path, minPartitions)
		//创建一个rdd,java中的RDD，都叫做JavaRDD，在RDD中，有元素的概念，每一个元素就相当于文件里的一行
		List<Tuple2<String,Integer>> number = Arrays.asList(new Tuple2<>("a",1),new Tuple2<>("a",21),new Tuple2<>("b",13),new Tuple2<>("b",11),new Tuple2<>("a",12));
		//JavaRDD<String> lines = jsc.textFile("J://Study//Spark//hello.txt");
		JavaPairRDD<String,Integer> lines = jsc.parallelizePairs(number);
				//.groupByKey().foreach(f ->System.out.println(f._1));;
				
		JavaPairRDD<String,Iterable<Integer>> groupKey=lines.groupByKey();
		groupKey.foreach(f -> {
			System.out.println(f._1);
			f._2.forEach(b -> System.out.println(b));
			/*Iterator<Integer> it = (Iterator<Integer>) f._2;
			while(it.hasNext()) {
				System.out.println(it.next());
			}*/
		});
		
		//Integer it = lines.reduce((a,b) -> a*2);
		//JavaRDD<Integer> number1 = lines.map(a -> {return a*2;});
		//JavaRDD<String> numberRDD = lines.flatMap(a -> Arrays.asList(a.split(" ")).iterator());
		
		//numberRDD.foreach(a ->{System.out.println(a);});
	    
			
		 
		jsc.close();
	}
	
	/**
	 * reduceByKey 根据key操作
	 */
	public static void reduceBykey() {
		SparkConf  sc = new SparkConf();
		sc.setAppName("WordCount");
		sc.setMaster("local");
		
		//sparkContext核心控件
		JavaSparkContext jsc = new JavaSparkContext(sc);
		//jsc.textFile(path, minPartitions)
		//创建一个rdd,java中的RDD，都叫做JavaRDD，在RDD中，有元素的概念，每一个元素就相当于文件里的一行
		List<Tuple2<String,Integer>> number = Arrays.asList(new Tuple2<>("a",1),new Tuple2<>("a",21),new Tuple2<>("b",13),new Tuple2<>("b",11),new Tuple2<>("a",12));
		//JavaRDD<String> lines = jsc.textFile("J://Study//Spark//hello.txt");
		JavaPairRDD<String,Integer> lines = jsc.parallelizePairs(number);
				//.groupByKey().foreach(f ->System.out.println(f._1));;
				
		JavaPairRDD<String,Integer> groupKey=lines.reduceByKey((a,b) ->{return a+b;});
		groupKey.foreach(f -> {
			
			System.out.println(f._1+":"+f._2);
			//f._2.forEach(b -> System.out.println(b));
			/*Iterator<Integer> it = (Iterator<Integer>) f._2;
			while(it.hasNext()) {
				System.out.println(it.next());
			}*/
		});
		
		//Integer it = lines.reduce((a,b) -> a*2);
		//JavaRDD<Integer> number1 = lines.map(a -> {return a*2;});
		//JavaRDD<String> numberRDD = lines.flatMap(a -> Arrays.asList(a.split(" ")).iterator());
		
		//numberRDD.foreach(a ->{System.out.println(a);});
	    
			
		 
		jsc.close();
	}
	/**
	 * sortByKey 根据key操作
	 */
	public static void sortBykey() {
		SparkConf  sc = new SparkConf();
		sc.setAppName("WordCount");
		sc.setMaster("local");
		
		//sparkContext核心控件
		JavaSparkContext jsc = new JavaSparkContext(sc);
		//jsc.textFile(path, minPartitions)
		//创建一个rdd,java中的RDD，都叫做JavaRDD，在RDD中，有元素的概念，每一个元素就相当于文件里的一行
		List<Tuple2<Integer,String>> number = Arrays.asList(new Tuple2<>(12,"a"),new Tuple2<>(21,"b"),new Tuple2<>(21,"b"),new Tuple2<>(13,"df"),new Tuple2<>(11,"daa"));
		//JavaRDD<String> lines = jsc.textFile("J://Study//Spark//hello.txt");
		JavaPairRDD<Integer,String> lines = jsc.parallelizePairs(number);
				//.groupByKey().foreach(f ->System.out.println(f._1));;
				
		JavaPairRDD<Integer,String> groupKey=lines.sortByKey(false);
		groupKey.foreach(f -> {
			
			System.out.println(f._1+":"+f._2);
			//f._2.forEach(b -> System.out.println(b));
			/*Iterator<Integer> it = (Iterator<Integer>) f._2;
			while(it.hasNext()) {
				System.out.println(it.next());
			}*/
		});
		
		//Integer it = lines.reduce((a,b) -> a*2);
		//JavaRDD<Integer> number1 = lines.map(a -> {return a*2;});
		//JavaRDD<String> numberRDD = lines.flatMap(a -> Arrays.asList(a.split(" ")).iterator());
		
		//numberRDD.foreach(a ->{System.out.println(a);});
	    
			
		 
		jsc.close();
	}
	/**
	 * join and cogroup
	 */
	public static void joinAndCongroup() {
		SparkConf  sc = new SparkConf();
		sc.setAppName("WordCount");
		sc.setMaster("local");
		
		//sparkContext核心控件
		JavaSparkContext jsc = new JavaSparkContext(sc);
		//jsc.textFile(path, minPartitions)
		//创建一个rdd,java中的RDD，都叫做JavaRDD，在RDD中，有元素的概念，每一个元素就相当于文件里的一行
		List<Tuple2<Integer,String>> number = Arrays.asList(new Tuple2<>(1,"a"),new Tuple2<>(2,"b"),new Tuple2<>(13,"df"),new Tuple2<>(3,"daa"));
		List<Tuple2<Integer,Integer>> number1 = Arrays.asList(new Tuple2<>(1,100),new Tuple2<>(1,200),new Tuple2<>(2,233),new Tuple2<>(13,44),new Tuple2<>(3,21));
		//JavaRDD<String> lines = jsc.textFile("J://Study//Spark//hello.txt");
		JavaPairRDD<Integer,String> lines = jsc.parallelizePairs(number);
		JavaPairRDD<Integer,Integer> lines2 = jsc.parallelizePairs(number1);
				//.groupByKey().foreach(f ->System.out.println(f._1));;
		
		//使用join关联RDD结果集
		JavaPairRDD<Integer,Tuple2<String,Integer>> str = lines.join(lines2);
		
		str.foreach(f ->{
			System.out.println(f._1+" :"+f._2._1+" "+f._2._2);
			
		});
		//JavaPairRDD<Integer,String> groupKey=lines.sortByKey(false);
		//groupKey.foreach(f -> {
			
			//System.out.println(f._1+":"+f._2);
			//f._2.forEach(b -> System.out.println(b));
			/*Iterator<Integer> it = (Iterator<Integer>) f._2;
			while(it.hasNext()) {
				System.out.println(it.next());
			}*/
		//});
		
		//Integer it = lines.reduce((a,b) -> a*2);
		//JavaRDD<Integer> number1 = lines.map(a -> {return a*2;});
		//JavaRDD<String> numberRDD = lines.flatMap(a -> Arrays.asList(a.split(" ")).iterator());
		
		//numberRDD.foreach(a ->{System.out.println(a);});
	    
			
		 
		jsc.close();
	}
	/**
	 * join and cogroup
	 */
	public static void Congroup() {
		SparkConf  sc = new SparkConf();
		sc.setAppName("WordCount");
		sc.setMaster("local");
		
		//sparkContext核心控件
		JavaSparkContext jsc = new JavaSparkContext(sc);
		//jsc.textFile(path, minPartitions)
		//创建一个rdd,java中的RDD，都叫做JavaRDD，在RDD中，有元素的概念，每一个元素就相当于文件里的一行
		List<Tuple2<Integer,String>> number = Arrays.asList(new Tuple2<>(1,"a"),new Tuple2<>(2,"b"),new Tuple2<>(13,"df"),new Tuple2<>(3,"daa"));
		List<Tuple2<Integer,Integer>> number1 = Arrays.asList(new Tuple2<>(1,100),new Tuple2<>(1,20),new Tuple2<>(2,213),new Tuple2<>(2,233),new Tuple2<>(13,44),new Tuple2<>(3,21),new Tuple2<>(3,31));
		//JavaRDD<String> lines = jsc.textFile("J://Study//Spark//hello.txt");
		JavaPairRDD<Integer,String> lines = jsc.parallelizePairs(number);
		JavaPairRDD<Integer,Integer> lines2 = jsc.parallelizePairs(number1);
				//.groupByKey().foreach(f ->System.out.println(f._1));;
		
		//使用join关联RDD结果集
		JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> str = lines.cogroup(lines2);
		
		str.foreach(f ->{
			System.out.println(f._1+" :"+f._2._1+" "+f._2._2);
			//f._2._1.forEach(b -> System.out.println(f._2));
			//f._2._2.forEach(b -> System.out.println(f._1);
		});
		//JavaPairRDD<Integer,String> groupKey=lines.sortByKey(false);
		//groupKey.foreach(f -> {
			
			//System.out.println(f._1+":"+f._2);
			//f._2.forEach(b -> System.out.println(b));
			/*Iterator<Integer> it = (Iterator<Integer>) f._2;
			while(it.hasNext()) {
				System.out.println(it.next());
			}*/
		//});
		
		//Integer it = lines.reduce((a,b) -> a*2);
		//JavaRDD<Integer> number1 = lines.map(a -> {return a*2;});
		//JavaRDD<String> numberRDD = lines.flatMap(a -> Arrays.asList(a.split(" ")).iterator());
		
		//numberRDD.foreach(a ->{System.out.println(a);});
	    
			
		 
		jsc.close();
	}
}