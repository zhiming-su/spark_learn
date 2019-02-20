package com.spark.wordcount.controller;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * UDAF用户自定义聚合函数 user fined Aggregate fucntion
 * 
 * @author suzhi 对多行输入进行返回输出
 *
 */
public class SparkSqlUDAF extends UserDefinedAggregateFunction {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	// 是指函数返回值的类型
	@Override
	public DataType dataType() {
		// TODO Auto-generated method stub
		return DataTypes.DoubleType;
	}

	// deterministic 确定性的
	@Override
	public boolean deterministic() {
		return true;
	}

	// 最后，指的是，一个分组的聚合值，如何通过中间的缓存聚合值，最后返回一个最终聚合值
	@Override
	public Double evaluate(Row buffer) {
		// TODO Auto-generated method stub
		return buffer.getDouble(0);
	}

	// 为每个分组的数据执行初始化操作
	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		// TODO Auto-generated method stub
		buffer.update(0, 0.0);
		//buffer.update(1, 0L);
	}

	// 是指输入数据的类型
	@Override
	public StructType inputSchema() {
		List<StructField> inputFields = new ArrayList<>();
		inputFields.add(DataTypes.createStructField("input", DataTypes.DoubleType, true));
		return DataTypes.createStructType(inputFields);
	}

	// 是指中间进行聚合时，所处理的数据类型
	@Override
	public StructType bufferSchema() {
		List<StructField> bufferFields = new ArrayList<>();
		bufferFields.add(DataTypes.createStructField("sum", DataTypes.DoubleType, true));
		//bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
		return DataTypes.createStructType(bufferFields);
	}

	// 由于spark是分布式，所以一个分组的数据，可能会在不同的节点上进行布局聚合，就是update
	// 但是最后的一个分组，在各个几点上的聚合要进行merge,也就是合并
	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0));
		//buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1));

	}

	// 每个分组，有信的值进来的时候，如何进行分组的聚合值的计算
	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		// TODO Auto-generated method stub
		
			buffer.update(0, input.getDouble(0) +1);
			
		
	}


}
