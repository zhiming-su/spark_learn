package com.spark.wordcount.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class JavaHDSFSStreaming {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		SparkConf sc = new SparkConf().setAppName("HDFSStreaming").setMaster("local[2]");
		
		JavaStreamingContext jsc = new JavaStreamingContext(sc,Durations.seconds(5));
		
		//hadoop fs -put /path/filename /hdfsstream
		JavaDStream<String> jds = jsc.textFileStream("hdfs://192.168.1.170:9000/hdfsstream/");
		jds.print();
		
		jsc.start();
		jsc.awaitTermination();
		jsc.close();

	}

}
