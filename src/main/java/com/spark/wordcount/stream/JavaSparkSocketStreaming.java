package com.spark.wordcount.stream;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class JavaSparkSocketStreaming {

	public static void mySocketTest() throws InterruptedException {
		//local 2 表示有多少进程
		SparkConf sc = new SparkConf().setAppName("socketStreaming").setMaster("local[2]");
		
		JavaStreamingContext jsc  = new JavaStreamingContext(sc,Durations.seconds(2));
		
		JavaReceiverInputDStream<String> jd = jsc.socketTextStream("192.168.1.173", 12345);
		
		JavaDStream<String> js1 = jd.flatMap(line->{ return Arrays.asList(line.split(" ")).iterator();
			});
		
		JavaPairDStream<String,Integer> jps = js1.mapToPair(lines ->{
			
			return new Tuple2<String,Integer>(lines,1);
		}).reduceByKey((i1,i2) -> i1+i2);
		
		jps.print();
		
		jsc.start();
		jsc.awaitTermination();
		jsc.close();
	}
	 public static void main(String[] args) throws Exception {
		 mySocketTest();
	 }
}
