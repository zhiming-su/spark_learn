package com.spark.wordcount.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * UDAF用户自定义聚合函数 user fined Aggregate fucntion
 * 
 * @author suzhi 对多行输入进行返回输出
 *
 */
public class SparkSqlUDAF_Use  {

	public static void myUDAF1() {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf()
				.setAppName("RDD2DataFrame")
				.setMaster("local");
		
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		
		List<String> list = Arrays.asList("Leo,1","Leo,2","java,2","Python,3","Perl,4");
		
		JavaRDD<Row> jrdd = jsc.parallelize(list).map(line ->{
			return RowFactory.create(line.split(",")[0],Integer.valueOf(line.split(",")[1]));
		});
		
		ArrayList<StructField> field = new ArrayList<StructField>();
		field.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		field.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
		
		StructType schema = DataTypes.createStructType(field);
		
		Dataset<Row> dtr = spark.createDataFrame(jrdd, schema);
		
		dtr.createOrReplaceTempView("pp");
		
		spark.udf().register("sum_my", new SparkSqlUDAF());
		
		spark.sql("select name ,sum_my(id) from pp group by name").show();
		
		dtr.select();
		jsc.close();
		spark.close();
	}

}
