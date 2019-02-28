package com.spark.wordcount.stream;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * window 滑动窗口 spark Streaming 比storm支持的好
 * 
 * @author suzhi
 *
 *         每隔5s,统计60s钟的频次，并打印前3个词的次数
 */
public class JavaStreamingWindow {

	public static void myWindow() throws InterruptedException {
		// TODO Auto-generated method stub
		SparkConf sc = new SparkConf().setAppName("sparkStreamingWindow").setMaster("local[3]");

		JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(1));

		JavaReceiverInputDStream<String> jrid = jsc.socketTextStream("192.168.1.173", 12345);

		JavaPairDStream<String, Integer> jpd = jrid
				.mapToPair(line -> new Tuple2<String, Integer>(line.split(" ")[1], 1));

		// 针对(searchWord, 1)的tuple格式的DStream，执行reduceByKeyAndWindow，滑动窗
		// 第二个参数，是窗口长度，这里是60秒
		// 第三个参数，是滑动间隔，这里是10秒
		// 也就是说，每隔10秒钟，将最近60秒的数据，作为一个窗口，进行内部的RDD的聚合，然后统一对一个RDD进行后续
		// 计算
		// 所以说，这里的意思，就是，之前的searchWordPairDStream为止，其实，都是不会立即进行计算的
		// 而是只是放在那里
		// 然后，等待我们的滑动间隔到了以后，10秒钟到了，会将之前60秒的RDD，因为一个batch间隔是，5秒，所以之前
		// 60秒，就有12个RDD，给聚合起来，然后，统一执行redcueByKey操作
		// 所以这里的reduceByKeyAndWindow，是针对每个窗口执行计算的，而不是针对某个DStream中的RDD

		JavaPairDStream<String, Integer> windowJPD = jpd.reduceByKeyAndWindow((line1, line2) -> line1 + line2,
				Durations.seconds(60), Durations.seconds(10));

		// 到这里为止，就已经可以做到，每隔10秒钟，出来，之前60秒的收集
		// 执行transform操作，因为，一个窗口，就是一个60秒钟的数
		// 根据每个搜索词出现的频率进行排序，然后获取排名前3的热点搜索词

		JavaPairDStream<String, Integer> finalWindow = windowJPD
				.transformToPair( new Function<JavaPairRDD<String,Integer>, JavaPairRDD<String,Integer>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> v1) throws Exception {
						// TODO Auto-generated method stub
						//rdd，k v反转
						JavaPairRDD<Integer, String> jpr = v1.mapToPair(line -> new Tuple2<Integer,String>(line._2,line._1));
						JavaPairRDD<String, Integer> jpr1 = jpr.sortByKey().mapToPair(line->new Tuple2<String,Integer>(line._2,line._1));
						List<Tuple2<String, Integer>> topRDD = jpr1.take(3);
						topRDD.forEach(f->System.out.println(f));
						
						return jpr1;
					}
				
				}
						);
		
		// 这个无关紧要，只是为了触发job的执行，所以必须有output操作
		finalWindow.print();

		jsc.start();
		jsc.awaitTermination();
		jsc.close();
	}

}
