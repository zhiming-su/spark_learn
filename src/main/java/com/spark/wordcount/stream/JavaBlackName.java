package com.spark.wordcount.stream;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 黑名单过滤系统 就是javaDStream 转为JavaRDD的过程
 * 
 * @author suzhi
 *
 */
public class JavaBlackName {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		SparkConf sc = new SparkConf().setAppName("blackNameList").setMaster("local[2]");
		JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));

		List<Tuple2<String, Boolean>> blackList = new ArrayList<Tuple2<String, Boolean>>();
		blackList.add(new Tuple2<String, Boolean>("Jade", true));
		blackList.add(new Tuple2<String, Boolean>("Jack", false));

		JavaPairRDD<String, Boolean> blackListRDD = jsc.sparkContext().parallelizePairs(blackList);

		// nc -lk 12345
		JavaReceiverInputDStream<String> jrid = jsc.socketTextStream("192.168.1.173", 12345);

		// return tuple2
		JavaPairDStream<String, String> jridLog = jrid
				.mapToPair(line -> new Tuple2<String, String>(line.split(" ")[1], line));

		// 与黑名单数据过滤
		JavaDStream<String> validAdsLog = jridLog
				.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public JavaRDD<String> call(JavaPairRDD<String, String> v1) throws Exception {
						// TODO Auto-generated method stub
						JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> leftRDD = v1.leftOuterJoin(blackListRDD);
						// 进行条件过滤
						JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> 	leftRDD1 = leftRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {

							/**
							 * 
							 */
							private static final long serialVersionUID = 1L;

							@Override
							public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> v2) throws Exception {
								// TODO Auto-generated method stub
								//isPresent 数据是存在的
								//System.out.println(v2._2._2.get()+ " "+v2._2._2.isPresent() );
								if(v2._2._2.isPresent() && v2._2._2.get()) {
									//System.out.println(v2._2._2.get());
									return false;
								}
								return true;
							}
						});
						JavaRDD<String> leftRDD2 =leftRDD1.map(line-> line._2._1);
						
						return leftRDD2;
					}

				});

		validAdsLog.print();
		
		jsc.start();
		jsc.awaitTermination();
		jsc.close();
	}

}
