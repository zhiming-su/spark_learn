package com.spark.wordcount.stream;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class JavaUpdateStateByKeyStream {

	public static void updateStateByKeyTest1() throws InterruptedException {
		SparkConf sc = new SparkConf().setAppName("updateStateByKey").setMaster("local[2]");
		JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));

		jsc.checkpoint("hdfs://192.168.1.170:9000/hdfsstream/");
		// nc -lk 12345nc
		JavaReceiverInputDStream<String> jrid = jsc.socketTextStream("192.168.1.173", 12345);

		JavaDStream<String> jds = jrid.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

		JavaPairDStream<String, Integer> jpds = jds.mapToPair(line -> new Tuple2<String, Integer>(line, 1));

		JavaPairDStream<String, Integer> wordCounts = jpds.updateStateByKey(

				// 这里的Optional，相当于Scala中的样例类，就是Option，可以这么理解
				// 它代表了一个值的存在状态，可能存在，也可能不存在
				new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

					private static final long serialVersionUID = 1L;

					// 这里两个参数
					// 实际上，对于每个单词，每次batch计算的时候，都会调用这个函数
					// 第一个参数，values，相当于是这个batch中，这个key的新的值，可能有多个吧
					// 比如说一个hello，可能有2个1，(hello, 1) (hello, 1)，那么传入的是(1,1)
					// 第二个参数，就是指的是这个key之前的状态，state，其中泛型的类型是你自己指定的
					@Override
					public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
						// 首先定义一个全局的单词计数
						Integer newValue = 0;

						// 其次，判断，state是否存在，如果不存在，说明是一个key第一次出现
						// 如果存在，说明这个key之前已经统计过全局的次数了
						if (state.isPresent()) {
							newValue = state.get();
						}

						// 接着，将本次新出现的值，都累加到newValue上去，就是一个key目前的全局的统计
						// 次数
						for (Integer value : values) {
							newValue += value;
						}

						return Optional.of(newValue);
					}

				});

		// JavaPairDStream<String,Integer> jpds1 = jpds.reduceByKey((line,line1) ->
		// line+line1);

		wordCounts.print();

		// jrid.print();

		jsc.start();
		jsc.awaitTermination();
		jsc.close();
	}
}
