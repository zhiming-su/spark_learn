package com.spark.wordcount.controller;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class SparkSql2 {

	
	//使用parallelize的方法来模式数据
	/**
	 * 分组求和，方法
	 */
	public static void sparkSqlGroupBy() {
		SparkConf sc = new SparkConf()
				.setAppName("groupBy")
				.setMaster("local");
		
		SparkSession spark = SparkSession.builder().config(sc).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		
		StringBuffer sb = new StringBuffer("2015-10-01,000");
		for(int i=0;i<1000000;i++) {
			sb.append(","+"2015-10-01,00"+i);
		}
		List<String>  info = Arrays.asList("2015-10-01,001,11","2015-10-01,001,12","2015-10-01,0013,11","2015-10-02,0012,13","2015-10-04,0012,13");
		//Dataset<Row> jrList = spark.createDataFrame(info, Students.class);
		//spark.cre
		
		
		JavaRDD<Row> jr = jsc.parallelize(info).map(line->{
			return RowFactory.create(line.split(",")[0],line.split(",")[1],line.split(",")[2]);
		});
		
		ArrayList<StructField> listSF = new ArrayList<StructField>();
		listSF.add(DataTypes.createStructField("time", DataTypes.StringType, true));
		listSF.add(DataTypes.createStructField("id", DataTypes.StringType, true));
		listSF.add(DataTypes.createStructField("age", DataTypes.StringType, true));
		
		StructType schema = DataTypes.createStructType(listSF);
		
		Dataset<Row> dsr = spark.createDataFrame(jr, schema);
		
		dsr.groupBy(dsr.col("id")).count().show();
		
		dsr.groupBy(dsr.col("id")).agg(max(dsr.col("age"))).show();
		
		//先分组，在使用聚合函数
		dsr.groupBy(dsr.col("id")).agg(countDistinct(dsr.col("age"))).show();
		
		jsc.close();
		spark.close();
	}
	
	/**
	 * rowNumber 
	 * 开窗函数
	 * @throws AnalysisException 
	 * //使用开窗函数row_number进行查询，查询每个种类销售额前三的产品
        DataFrame top3SalesDF = hiveContext.sql("SELECT product, category, revenue from ("
                      + "SELECT product, category, revenue, "
                      //row_number语法说明：
                      //1、在使用select查询时使用row_number()函数
                      //2、在row_number()之后跟上OVER关键字
                      //3、然后是()，里面是PARTITION BY，指定根据哪个字段分组
                      //4、分组之后，可以使用ORDER BY对每个分组进行排序
                      //5、之后row_number()，就会对分组后的每行给一个组内行号，之后就可以取行号内前3的行
                      + "row_number() OVER (PARTITION BY category ORDER BY revenue DESC) rank "
                      + "FROM sales) t "
                      + "WHERE rank <= 3");
	 * 
	 */
	public static void rowNumberTopN() throws AnalysisException {
		
		SparkConf conf = new SparkConf()
				.setAppName("RDD2DataFrame")
				.setMaster("local");
		
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		
		JavaRDD<String> dt = spark.read().textFile("J://Study//Spark//sales.txt").javaRDD();
		
		JavaRDD<Row> jrddr = dt.map(lines ->{
			return RowFactory.create(lines.split("")[0],lines.split("")[1],lines.split("")[2]);
		});
		
		ArrayList<StructField> field = new ArrayList<StructField>();
		field.add(DataTypes.createStructField("mc", DataTypes.StringType, true));
		field.add(DataTypes.createStructField("lx", DataTypes.StringType, true));
		field.add(DataTypes.createStructField("price", DataTypes.StringType, true));
		
		StructType schema = DataTypes.createStructType(field);
		
		Dataset<Row> dtRow = spark.createDataFrame(jrddr, schema);
		
		dtRow.show();
		
		/**
+---------+----------+-----+-------+
|       mc|        lx|price|row_num|
+---------+----------+-----+-------+
|   Normal|    Tablet| 1500|      1|
|      Big|    Tablet| 2500|      2|
|      Pro|    Tablet| 4500|      3|
|     Mini|    Tablet| 5500|      4|
|     Pro2|    Tablet| 6500|      5|
|  Bedable|Cell Phone| 3000|      1|
| Foldable|Cell Phone| 3500|      2|
|UltraThin|Cell Phone| 5000|      3|
|     Thin|Cell Phone| 6000|      4|
| VeryThin|Cell Phone| 6000|      5|
+---------+----------+-----+-------+
		 */
		Dataset<Row> dtrow = dtRow.select(dtRow.col("mc"),row_number().over(Window.partitionBy("lx").orderBy("price")).alias("row_num"));
		//dtRow.createTempView("phone");
		
		
		//Join de tablas en las que comparten ciudad
		//fairport.join(dfairport_city_state, dfairport.col("leg_city").equalTo(dfairport_city_state.col("city")));
		dtrow.join(dtRow,dtRow.col("mc").equalTo(dtrow.col("mc")),"left").show();
		
		dtrow.createOrReplaceTempView("v1");
		dtRow.createOrReplaceTempView("v2");
		
		spark.sql("select a.*,b.* from v1  a left join v2 b on a.mc=b.mc where a.row_num<=3").show();
		
		
		spark.close();
	}
}
