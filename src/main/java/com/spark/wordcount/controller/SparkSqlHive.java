package com.spark.wordcount.controller;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * hive 数据源的处理
 * 
 * @author suzhi
 *
 */
public class SparkSqlHive {

	/**
	 * hive-site.xml
	 * 注意
	 * hive-site.xml默认配置的数据库连接方式就是HikariCP。
datanucleus.connectionPoolingType
HikariCP
直接改成dbcp

在hvie-site.xml中关闭版本验证
Hive Schema version 1.2.0 does not match metastore's schema version 3.1.0 Metastore is not upgraded or corrupt)
<property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
</property>
	 */
	public static void hiveSql() {
		// SparkConf conf = new
		// SparkConf().setAppName("RDD2DataFrame").setMaster("local");
		
		// warehouseLocation points to the default location for managed databases and tables
		String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
		SparkConf conf = new SparkConf().setAppName("RDD2DataFrame").setMaster("local");
		SparkSession spark = SparkSession.builder().config(conf)
				.config("spark.sql.warehouse.dir", warehouseLocation)
				.enableHiveSupport().getOrCreate();

		// file为本地模式，默认是hdfs方式

		// Dataset<Row> srdd = spark.read().json("J://Study//Spark //students.json");
		spark.sql("create table spark_students (id int, name string,age int)   row format delimited fields terminated by ' ' stored as textfile");
		spark.sql("LOAD DATA LOCAL INPATH '/opt/spark_test/spark_sql/students.txt' INTO TABLE spark_students");
		spark.sql("select * from spark_students").show();
		

		spark.close();
	}
}
