package com.spark.wordcount;

import java.io.IOException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.spark.wordcount.controller.SparkActionTest;
import com.spark.wordcount.controller.SparkController;
import com.spark.wordcount.controller.SparkController24;
import com.spark.wordcount.controller.SparkControllerLocal;
import com.spark.wordcount.controller.SparkGongXiangBianLiang;
import com.spark.wordcount.controller.SparkKeyValueTest;
import com.spark.wordcount.controller.SparkMapLocal;
import com.spark.wordcount.controller.SparkParallelizeLocal;
import com.spark.wordcount.controller.SparkRDDTest;
import com.spark.wordcount.controller.SparkTransformationTest;

@SpringBootApplication
public class Application {

	public static void main(String[] args) throws IOException {
		//SpringApplication.run(Application.class, args);
		//SparkControllerLocal.sparkConf();
		//SparkController24.sparkConf();
		SparkGongXiangBianLiang.gonxiangbianliangAccumlator();
	}
}
