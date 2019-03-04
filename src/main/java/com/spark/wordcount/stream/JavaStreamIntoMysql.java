package com.spark.wordcount.stream;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * streaming into mysql
 * 
 * @author suzhi
 *
 */
public class JavaStreamIntoMysql {
	public static void main(String[] args) throws Exception {
		SparkConf sc = new SparkConf().setAppName("StreamIntoMysql").setMaster("local[2]");
		
		JavaStreamingContext jsc = new JavaStreamingContext(sc,Durations.seconds(2));
		
		JavaReceiverInputDStream<String>  jrid = jsc.socketTextStream("192.168.1.173", 12345);
		
		//遍历每个RDD
		jrid.foreachRDD((javaRDD,time)->{
			SparkSession spark = MyJavaSparkSessionSingleton.getInstance(javaRDD.context().getConf());
			JavaRDD<EtlJob> etlRDD = javaRDD.map(line-> {
				EtlJob etl = new EtlJob();
				etl.setMASTER_ID(line.split(" ")[1]);
				etl.setJOB_ID(line.split(" ")[0]);
				return etl;
			});
			Dataset<Row> ds = spark.createDataFrame(etlRDD, EtlJob.class);
			//ds.show();
			Properties connectionProperties = new Properties();
			connectionProperties.put("user", "etl");
			connectionProperties.put("password", "etl_xiyu123");
			
			// rewriteBatchedStatements=true增加mysql 插入的速度增加
			ds.write().mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.1.22:3306?rewriteBatchedStatements=true",
					"database.ETL_JOB_copy", connectionProperties);
			System.out.println("========= " + time + "=========");
		});
		//JavaDStream<String> jd = jrid;
		//jd.print();
		
		jsc.start();
		jsc.awaitTermination();
		jsc.close();
	}
}
