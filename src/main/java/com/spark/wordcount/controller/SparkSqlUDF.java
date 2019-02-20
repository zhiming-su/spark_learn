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
 * 
 * @author suzhi
 *自定义UDF函数（用户自己的函数）
 */
public class SparkSqlUDF {

	public static void myUDF() {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf()
				.setAppName("RDD2DataFrame")
				.setMaster("local");
		
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		
		List<String> list = Arrays.asList("Leo","java","Python","Perl");
		
		JavaRDD<Row> jrdd = jsc.parallelize(list).map(line ->{
			return RowFactory.create(line);
		});
		
		ArrayList<StructField> field = new ArrayList<StructField>();
		field.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		
		StructType schema = DataTypes.createStructType(field);
		
		Dataset<Row> dtr = spark.createDataFrame(jrdd, schema);
		
		dtr.createOrReplaceTempView("pp");
		
		spark.udf().register("getlength", new UDF1<String,String>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			  public String call(String str) {
			    return String.valueOf(str.length());
			  }
		},DataTypes.StringType);
		spark.udf().register("getlength1", new UDF2<String,String,String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public String call(String str,String str1) {
			    return String.valueOf(str.length()+str1);
			  }
		},DataTypes.StringType);
		
		spark.sql("select name ,getlength(name),getlength1(name,name) from pp ").show();
		
		dtr.select();
		jsc.close();
		spark.close();
	}

	public Integer getLength(String str) {
		return str.length();
	}
}
