package com.spark.wordcount.controller;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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


spark中Dataset的的saveAsTable方法可以把数据持久化到hive中，其默认是用parquet格式保存数据文件的，若是想让其保存为其他格式，可以用format方法配置。

如若想保存的数据文件格式为hive默认的纯文本文件：

df.write.mode(SaveMode.Append).format("hive").saveAsTable("test")
1
format支持的格式有：

hive （hive默认格式，数据文件纯文本无压缩存储）
parquet （spark默认采用格式）
orc
json
csv
text（若用saveAsTable只能保存只有一个列的df）
jdbc
libsvm

	 */
	public static void hiveSql() {
		// SparkConf conf = new
		// SparkConf().setAppName("RDD2DataFrame").setMaster("local");
		
		// warehouseLocation points to the default location for managed databases and tables
		String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
		SparkConf conf = new SparkConf().setAppName("RDD2DataFrame").setMaster("local");
		SparkSession spark = SparkSession.builder().config(conf)
				.config("spark.sql.warehouse.dir", warehouseLocation)
				.enableHiveSupport()//开启hive支持
				.getOrCreate();

		// file为本地模式，默认是hdfs方式

		// Dataset<Row> srdd = spark.read().json("J://Study//Spark //students.json");
		spark.sql("create table spark_students (id int, name string,age int)   row format delimited fields terminated by ' ' stored as textfile");
		spark.sql("LOAD DATA LOCAL INPATH '/opt/spark_test/spark_sql/students.txt' INTO TABLE spark_students");
		spark.sql("select * from spark_students").show();
		
		//保存到hive中
		Dataset<Row> ds = spark.sql("select * from students");
		ds.show();
		ds.write().format("hive").saveAsTable("spark_hive");
		
		

		spark.close();
	}
}
