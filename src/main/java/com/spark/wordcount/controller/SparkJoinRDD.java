package com.spark.wordcount.controller;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class SparkJoinRDD {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf sc = new SparkConf().setAppName("RDDJOIN").setMaster("local");
		
		SparkSession ss = SparkSession.builder().config(sc).getOrCreate();
		
		JavaRDD<String> jr = ss.read().textFile("J://Study//Spark//score.txt").javaRDD();
		JavaRDD<String> jr3 = ss.read().textFile("J://Study//Spark//score3.txt").javaRDD();
		JavaPairRDD<String,String> jrt = jr.mapToPair(line -> new Tuple2<String,String>(line.split(" ")[0],line.split(" ")[1]));
		
		//rdd数据合并
		JavaRDD<String> jr4 = jr.union(jr3);
		jr4.foreach(line->System.out.println(line));
		System.out.println("-------");
		
		JavaRDD<String> jr2 = ss.read().textFile("J://Study//Spark//score2.txt").javaRDD();
		JavaPairRDD<String,String> jrt2 = jr2.mapToPair(line -> new Tuple2<String,String>(line.split(" ")[0],line.split(" ")[1]));
		
		//RDD中不能添加新的数据，只能union合并数据，形成一个新的RDD,或者使用此方法
		/**
		 * public JavaInputDStream<Object> FillDStream() {
    LinkedList<RDD<Object>> rdds = new LinkedList<RDD<Object>>();
    rdds.add(context.sparkContext.emptyRDD());
    rdds.add(context.sparkContext.emptyRDD());

    JavaInputDStream<Object> filledDStream = context.queueStream(rdds);
    return filledStream;
}
		 */
		JavaPairRDD<String,String> jrt3 = jrt.union(jrt2);
		
		//jrt3.foreach(line-> System.out.println(line));
		
		JavaRDD<String> jr1 = ss.read().textFile("J://Study//Spark//score1.txt").javaRDD();
		JavaPairRDD<String,String> jrt1 = jr1.mapToPair(line -> new Tuple2<String,String>(line.split(" ")[0],line.split(" ")[1]));
		
		
		
		JavaPairRDD<String, Tuple2<String, Optional<String>>>  jpd1 = jrt3.leftOuterJoin(jrt1);
		
		
		//jrt1.foreach(line-> System.out.println(line));
		
		jpd1.foreach(line-> System.out.println(line._1 +" "+line._2._1+" "+line._2._2.orNull()));
		System.out.println("-------");
		//去掉为空的数据
		JavaPairRDD<String, Tuple2<String, Optional<String>>>  jpd2 = jpd1.filter(line-> line._2._2.isPresent());
		
	    jpd2.foreach(line-> System.out.println(line._1 +" "+line._2._1+" "+line._2._2.orNull()));
		
		ss.close();
	}

}
