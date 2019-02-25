package com.spark.wordcount.controller;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/*
 * 数据格式：
日期 用户 搜索词 城市 平台 版本

需求：
1、筛选出符合查询条件（城市、平台、版本）的数据
2、统计出每天搜索uv排名前3的搜索词
3、按照每天的top3搜索词的uv搜索总次数，倒序排序
4、将数据保存到hive表中

 */
public class SparkSqlUV {

	public static void myFunUV() {
		SparkConf conf = new SparkConf()
				.setAppName("myFunUV")
				.setMaster("local");
		
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		JavaRDD<String> jrdd = spark.read().textFile("J://Study//Spark//keyword.txt").javaRDD();
		
		JavaRDD<Row> jrddR = jrdd.map(line ->{
			return RowFactory.create(line.split("\\s")[0],
					line.split("\\s")[1],line.split("\\s")[2],line.split("\\s")[3],line.split("\\s")[4],Double.valueOf(line.split("\\s")[5]));
		});
		ArrayList<StructField> field = new ArrayList<StructField>();
		field.add(DataTypes.createStructField("time", DataTypes.StringType, true));
		field.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		field.add(DataTypes.createStructField("world", DataTypes.StringType, true));
		field.add(DataTypes.createStructField("region", DataTypes.StringType, true));
		field.add(DataTypes.createStructField("phone", DataTypes.StringType, true));
		field.add(DataTypes.createStructField("version", DataTypes.DoubleType, true));
		
		
		StructType schema = DataTypes.createStructType(field);
		
		Dataset<Row> dtr = spark.createDataFrame(jrddR, schema);
		
	    dtr.createOrReplaceTempView("info");
	    
	    spark.sql("select time,name,world,count(world) from info group by time,name,world order by time").show();
		
	}
	
}
