package com.spark.wordcount.stream;

import java.util.ArrayList;
import java.util.List;


import static org.apache.spark.sql.functions.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class JavaStreamingWindowTop3 {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		SparkConf sc = new SparkConf().setAppName("hotwidowTop3").setMaster("local[2]");
		JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));
		
		JavaReceiverInputDStream<String> jrid =jsc.socketTextStream("192.168.1.173", 12345);
		
		JavaPairDStream<String, Integer> jpd=jrid.mapToPair(line-> new Tuple2<String,Integer>(line,1));
		
		//进行reduceBykey window操作
		JavaPairDStream<String, Integer> windowJPD=jpd.reduceByKeyAndWindow((line,line1)-> line+line1,Durations.seconds(60), Durations.seconds(10));
		
		//foreachRDD
		windowJPD.foreachRDD((windRDD,time)->{
			//transforma to row
			JavaRDD<Row> rdd = windRDD.map(line-> RowFactory.create(line._1.split(" ")[0],line._1.split(" ")[1],line._2));
			List<StructField> fiels = new ArrayList<StructField>();
			fiels.add(DataTypes.createStructField("lx", DataTypes.StringType, true));
			fiels.add(DataTypes.createStructField("product", DataTypes.StringType, true));
			fiels.add(DataTypes.createStructField("num", DataTypes.IntegerType, true));
			
			StructType schema =DataTypes.createStructType(fiels);
			
			SparkSession spark = MyJavaSparkSessionSingleton.getInstance(windRDD.context().getConf());
			Dataset<Row> ds = spark.createDataFrame(rdd, schema);
			ds.select(ds.col("lx"),ds.col("product"),ds.col("num"),row_number().over(Window.partitionBy("lx").orderBy("num")).alias("row")).show();
			/**
			 * +---+-------+---+---+
| lx|product|num|row|
+---+-------+---+---+
|  c| iphone|  1|  1|
|  b| iphone|  2|  1|
|  a| huawei|  1|  1|
|  a| iphone|  1|  2|
|  a|   jarv|  1|  3|
|  a|    jar|  1|  4|
|  a|      b|  1|  5|
|  a|  nokia|  1|  6|
+---+-------+---+---+
			 */
		});
		
		jsc.start();
		jsc.awaitTermination();
		jsc.close();
	}

}
