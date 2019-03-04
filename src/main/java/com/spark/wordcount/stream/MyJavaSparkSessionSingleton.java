package com.spark.wordcount.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class MyJavaSparkSessionSingleton {
	/** Lazily instantiated singleton instance of SparkSession */
	
	  private static transient SparkSession instance = null;
	  public static SparkSession getInstance(SparkConf sparkConf) {
	    if (instance == null) {
	      instance = SparkSession
	        .builder()
	        .config(sparkConf)
	        .getOrCreate();
	    }
	    return instance;
	  }
	
}
