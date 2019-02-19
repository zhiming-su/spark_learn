package com.spark.wordcount.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * 这里主要对比Dataset和DataFrame，因为Dataset和DataFrame拥有完全相同的成员函数，区别只是每一行的数据类型不同
 * 
 * DataFrame也可以叫Dataset[Row],每一行的类型是Row，不解析，每一行究竟有哪些字段，各个字段又是什么类型都无从得知，
 * 只能用上面提到的getAS方法或者共性中的第七条提到的模式匹配拿出特定字段
 * 
 * 而Dataset中，每一行是什么类型是不一定的，在自定义了case class之后可以很自由的获得每一行的信息
 * 
 * 该什么时候使用 DataFrame 或 Dataset 呢？ 如果你需要丰富的语义、高级抽象和特定领域专用的 API，那就使用 DataFrame 或
 * Dataset； 如果你的处理需要对半结构化数据进行高级处理，如 filter、map、aggregation、average、sum、SQL
 * 查询、列式访问或使用 lambda 函数，那就使用 DataFrame 或 Dataset； 如果你想在编译时就有高度的类型安全，想要有类型的 JVM
 * 对象，用上 Catalyst 优化，并得益于 Tungsten 生成的高效代码，那就使用 Dataset； 如果你想在不同的 Spark
 * 库之间使用一致和简化的 API，那就使用 DataFrame 或 Dataset； 如果你是 R 语言使用者，就用 DataFrame； 如果你是
 * Python 语言使用者，就用 DataFrame，在需要更细致的控制时就退回去使用 RDD；
 * 
 * @author suzhi
 *
 */

@Component
@RestController
public class SparkSql {

	public static void dataFrameFunJson() {

		SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example Json").master("local")
				// .config("spark.some.config.option", "some-value")
				.getOrCreate();

		// spark.logInfo(null);
		// Creates a DataFrame based on a table named "people"

		Dataset<Row> df = spark.read().json("J://Study//Spark//students.json");
		df.createOrReplaceTempView("js");
		// spark.sql("select * from js where age=18").show();
		df.show();
		df.printSchema();

		df.select("age").show();

		df.select(df.col("name"), df.col("age").plus(1)).show();

		df.filter(df.col("age").gt(18)).show();
		spark.sql("select * from js where age>17").show();
		// df.filter(df.col("age").gt("18")).show();

		df.groupBy(df.col("age")).count().show();

		spark.close();

	}

	/**
	 * 使用dataset 方式
	 */
	public static void dataSet() {
		SparkConf conf = new SparkConf().setAppName("RDD2DataFrame").setMaster("local");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		Students sts = new Students();
		sts.setAge(12);
		sts.setId(1);
		sts.setName("dfad");

		Encoder<Students> studentsEncoder = Encoders.bean(Students.class);

		Dataset<Students> stDS = spark.createDataset(Collections.singletonList(sts), studentsEncoder);

		stDS.show();

		// Encoders for most common types are provided in class Encoders
		Encoder<Integer> integerEncoder = Encoders.INT();
		Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
		Dataset<Integer> transformedDS = primitiveDS.map((MapFunction<Integer, Integer>) value -> value + 1,
				integerEncoder);
		transformedDS.collect().toString(); // Returns [2, 3, 4]

		// DataFrames can be converted to a Dataset by providing a class. Mapping based
		// on name
		String path = "J://Study//Spark//students.json";
		Dataset<Students> peopleDS = spark.read().json(path).as(studentsEncoder);
		peopleDS.show();
		peopleDS.printSchema();

	}

	// mysql数据源
	// validation-api-1.1.0.Final.jar 与spark的包相冲突
	@RequestMapping(value = "/spark_mysql", method = RequestMethod.GET)
	public static String dataFrameFunMysql() throws AnalysisException {

		SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example").master("local")
				// .config("spark.some.config.option", "some-value")
				.getOrCreate();

		// Creates a DataFrame based on a table named "people"
		// stored in a MySQL database.
		String url = "jdbc:mysql://192.168.1.22:3306/database?user=etl;password=etl_xiyu123";
		Dataset<Row> df = spark.read().format("jdbc").option("url", url).option("dbtable", "ETL_JOB").load();

		df.createTempView("etl_job");

		Dataset<Row> mysqlDf = spark.sql("select JOB_ID,MASTER_ID from etl_job ");
		// Looks the schema of this DataFrame.
		// df.printSchema();
		df.show(100);
		df.count();
		
		//reanme the columns
		//mysqlDf.withColumnRenamed(existingName, newName)
		// Counts people by age
		// Dataset<Row> countsByAge = df.groupBy("MASTER_ID").count();
		// countsByAge.show();

		// Saves countsByAge to S3 in the JSON format.
		// countsByAge.write().format("json").save("s3a://...");

		// String to Row
		//dataset row 转换成students对象
		JavaRDD<Students> ds = mysqlDf.javaRDD().map(line -> {
			Students sts = new Students();
			sts.setId(Integer.valueOf(line.get(0).toString()));
			sts.setName(line.get(1).toString());
			return sts;
		});

		Dataset<Row> dsr = spark.createDataFrame(ds, Students.class);
		dsr.show();
		// save date into jdbc

		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "etl");
		connectionProperties.put("password", "etl_xiyu123");
		// Dataset<Row> jdbcDF2 = spark.read()
		// .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);

		// Saving data to a JDBC source
		/*
		 * df.write() .format("jdbc") .option("url", "jdbc:postgresql:dbserver")
		 * .option("dbtable", "schema.tablename") .option("user", "username")
		 * .option("password", "password") .save();
		 */

		// rewriteBatchedStatements=true增加mysql 插入的速度增加
		dsr.write().mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.1.22:3306?rewriteBatchedStatements=true",
				"database.ETL_JOB_copy1", connectionProperties);

		// Specifying create table column data types on write
		/*
		 * df.write() .option("createTableColumnTypes",
		 * "name CHAR(64), comments VARCHAR(1024)") .jdbc("jdbc:postgresql:dbserver",
		 * "schema.tablename", connectionProperties);
		 */
		// $example off:jdbc_dataset$
		spark.stop();
		return "OK";
	}

	/**
	 * RDD to Dataframe 使用反射的方式转换
	 * 
	 */
	public static void RddToDataFrame() {
		SparkConf conf = new SparkConf().setAppName("RDD2DataFrame").setMaster("local");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		JavaRDD<String> srdd = spark.read().textFile("J://Study//Spark//students.txt").javaRDD();

		JavaRDD<Students> ds = srdd.map(line -> {
			Students student = new Students();
			student.setAge(Integer.valueOf(line.split(",")[2]));
			student.setId(Integer.valueOf(line.split(",")[0]));
			student.setName(line.split(",")[1]);
			return student;
		});
		Dataset<Row> dsr = spark.createDataFrame(ds, Students.class);
		dsr.show();
		dsr.select("id", "name", "age").show();
		dsr.createOrReplaceTempView("students");
		spark.sql("select id,name,age from students where age<=18").show();

		// dataframe to rdd
		JavaRDD<Row> jr = dsr.javaRDD();

		JavaRDD<Students> jrStudent = jr.map(line -> {
			Students st = new Students();
			st.setAge(line.getInt(0));
			st.setName(line.getString(2));
			st.setId(line.getInt(1));
			return st;
		});

		// jrStudent.cl
		jrStudent.foreach(f -> {
			System.out.println(f.getAge() + " " + f.getName() + " " + f.getId());
		});
	}

	/**
	 * 动态反转 在不清楚bean对象属性类型的时候使用动态转换的方式
	 */
	public static void RddToDataFrame2() {
		SparkConf conf = new SparkConf().setAppName("RDD2DataFrame").setMaster("local");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		JavaRDD<String> srdd = spark.read().textFile("J://Study//Spark//students.txt").javaRDD();

		// String to Row
		JavaRDD<Row> ds = srdd.map(line -> {
			return RowFactory.create(Integer.valueOf(line.split(",")[0]), line.split(",")[1],
					Integer.valueOf(line.split(",")[2]));
		});

		ArrayList<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

		StructType schema = DataTypes.createStructType(fields);

		Dataset<Row> dsr = spark.createDataFrame(ds, schema);

		dsr.show();
		// dsr.coalesce(1).write().mode(SaveMode.Append).parquet("parquet.res1");
		dsr.printSchema();

		/**
		 * 第六步：对结果进行处理，包括由DataFrame转换成为RDD<Row>，以及结构持久化 this method should only be used
		 * if the resulting array is expected to be small, asall the data is loaded into
		 * the driver's memory.
		 */
		List<Row> listRow = dsr.javaRDD().collect();
		for (Row row : listRow) {
			System.out.println(row);
		}
		spark.close();
	}

	/**
	 * 
	 * save方法，对结果进行保存 删掉本地spark环境下 的gson2.4.2.jar文件，和spring boot 文件造成了冲突
	 * SaveMode.Ignore 存储方式 Append Overwrite ErrorIfExists（默认） Ignore（忽略）
	 */
	public static void saveData() {
		// SparkConf conf = new
		// SparkConf().setAppName("RDD2DataFrame").setMaster("local");
		SparkConf conf = new SparkConf().setAppName("RDD2DataFrame");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// file为本地模式，默认是hdfs方式

		Dataset<Row> srdd = spark.read().json("file:///opt/spark_test/spark_sql/students.json");
		srdd.select("id").write().save("file:///opt/spark_test/spark_sql/students.csv");
		srdd.write().mode(SaveMode.Ignore).option("header", true).csv("file:///opt/spark_test/spark_sql/students.csv");
		spark.close();
	}

	/**
	 * 加载parquet数据
	 */
	public static void readParquet() {
		SparkConf conf = new SparkConf().setAppName("RDD2DataFrame");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// file为本地模式，默认是hdfs方式

		Dataset<Row> srdd = spark.read().parquet(
				"/opt/spark_test/spark_sql/students.csv/part-00000-0da6d00b-5909-4542-8c88-2201dd0731f9-c000.snappy.parquet");
		srdd.show();
	}

	/**
	 * json jion文件
	 */
	public static void jsonJoin() {
		// SparkConf conf = new
		// SparkConf().setAppName("RDD2DataFrame").setMaster("local");
		SparkConf conf = new SparkConf().setAppName("RDD2DataFrame").setMaster("local");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// file为本地模式，默认是hdfs方式

		Dataset<Row> srdd = spark.read().json("J://Study//Spark//students.json");
		Dataset<Row> srdd1 = spark.read().json("J://Study//Spark//class.json");
		srdd.createOrReplaceTempView("students");
		srdd1.createOrReplaceTempView("class");
		spark.sql("select a.*,b.name as classname from students as a left join class as b on a.id=b.id").show();
 
		spark.close();
	}
}
