package com.spark.wordcount.controller;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;


//@Component
//系统初始化时，将所有ING执行的变成ERR
@Order(value = 0)
public class InitSystemConfig implements CommandLineRunner, EnvironmentAware {

    /*
     * 在服务启动后执行，会在@Bean实例化之后执行，故如果@Bean需要依赖这里的话会出问题
     */
    @Override
    public void run(String... args) {
    	SparkConf sc = new SparkConf()
				.setAppName("selectApp");
				//.setMaster("local[1]");
				//.setMaster("yarn-cluster");
				//.setMaster("spark://spark01:7077");
		//spark standalon 模式
		
    	SparkSession spark = SparkSession.builder().config(sc).getOrCreate();
    }

    /*
     * 在SystemConfigDao实例化之后、@Bean实例化之前执行
     * 常用于读取数据库配置以供其它bean使用
     * environment对象可以获取配置文件的配置，也可以把配置设置到该对象中
     */
    @Override
    public void setEnvironment(Environment environment) {

    }
}
