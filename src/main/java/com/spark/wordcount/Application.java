package com.spark.wordcount;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.spark.sql.AnalysisException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.spark.wordcount.controller.SparkETLJob;
import com.spark.wordcount.controller.SparkSql;
import com.spark.wordcount.controller.SparkSqlUDAF_Use;
import com.spark.wordcount.controller.SparkSqlUV;
import com.spark.wordcount.stream.JavaDirectKafkaWordCount;
import com.spark.wordcount.stream.JavaStreamingWindow;
import com.spark.wordcount.stream.JavaUpdateStateByKeyStream;


@SpringBootApplication
public class Application {

	public static void main(String[] args) throws IOException, AnalysisException, InterruptedException, ExecutionException {
		SpringApplication.run(Application.class, args);
		//SparkControllerLocal.sparkConf();
		//SparkController24.sparkConf();
		//SparkSql.dataFrameFunJson();
		//SparkSql.RddToDataFrame2();
		//SparkSql.dataFrameFunJson();
		//SparkSql.saveData();
		//SparkSql.readParquet();
		//SparkSql.jsonJoin();
		//SparkSqlHive.hiveSql();
		//SparkSql.dataFrameFunMysql();
		//SparkSql2.sparkSqlGroupBy();
		//SparkSqlUDF.myUDF();
		//SparkSqlUDAF_Use.myUDAF1();
		//SparkSqlUV.myFunUV();
		//SparkETLJob.etlJob();
		//JavaDirectKafkaWordCount.kafkaTest();
		//JavaUpdateStateByKeyStream.updateStateByKeyTest1();
		JavaStreamingWindow.myWindow();
		//SparkSecondSort.secondSort();
	}
}
