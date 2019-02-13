package com.spark.wordcount.controller;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 这里主要对比Dataset和DataFrame，因为Dataset和DataFrame拥有完全相同的成员函数，区别只是每一行的数据类型不同
 * 
 * DataFrame也可以叫Dataset[Row],每一行的类型是Row，不解析，每一行究竟有哪些字段，各个字段又是什么类型都无从得知，
 * 只能用上面提到的getAS方法或者共性中的第七条提到的模式匹配拿出特定字段
 * 
 * 而Dataset中，每一行是什么类型是不一定的，在自定义了case class之后可以很自由的获得每一行的信息
 * 
 * @author suzhi
 *
 */
public class SparkSql {

	public static void dataFrameFunJson() {

		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example Json")
				  .master("local")
				  //.config("spark.some.config.option", "some-value")
				  .getOrCreate();
		
		//spark.logInfo(null);
		// Creates a DataFrame based on a table named "people"
	
		Dataset<Row> df = spark.read().json("J://Study//Spark//students.json");
		//df.createOrReplaceTempView("js");
		//spark.sql("select * from js where age=18").show();
		df.show();
		df.printSchema();
		
		df.select("age").show();
		
		df.select(df.col("name"),df.col("age").plus(1)).show();
		
	   df.filter(df.col("age").gt(18)).show();
	   
	   df.groupBy(df.col("age")).count().show();
		
		spark.close();

	}

	// mysql数据源
	public static void dataFrameFunMysql() {

		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example")
				  .master("local")
				  //.config("spark.some.config.option", "some-value")
				  .getOrCreate();
		
		// Creates a DataFrame based on a table named "people"
		// stored in a MySQL database.
		String url = "jdbc:mysql://192.168.1.22:3306/database?user=etl;password=etl_xiyu123";
		Dataset<Row> df = spark.read().format("jdbc").option("url", url).option("dbtable", "ETL_JOB").load();
		
		// Looks the schema of this DataFrame.
		//df.printSchema();
		df.show(100);
		df.count();
		// Counts people by age
		//Dataset<Row> countsByAge = df.groupBy("MASTER_ID").count();
		//countsByAge.show();

		// Saves countsByAge to S3 in the JSON format.
		//countsByAge.write().format("json").save("s3a://...");
		spark.stop();
	}
}
