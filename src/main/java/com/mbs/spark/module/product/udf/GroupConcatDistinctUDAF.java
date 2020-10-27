package com.mbs.spark.module.product.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * 组内拼接去重函数（group_concat_distinct()）
 *
 * 技术点4：自定义UDAF聚合函数
 *
 */
public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {

	private static final long serialVersionUID = -2510776241322950505L;

	// 指定输入数据的字段与类型
	private StructType inputSchema = DataTypes.createStructType(Arrays.asList(
			DataTypes.createStructField("cityInfo", DataTypes.StringType, true)));
	// 指定缓冲数据的字段与类型
	private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(
			DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)));
	// 指定返回类型
	private DataType dataType = DataTypes.StringType;
	// 指定是否是确定性的
	private boolean deterministic = true;

	@Override
	public StructType inputSchema() {
		return inputSchema;
	}

	@Override
	public StructType bufferSchema() {
		return bufferSchema;
	}

	@Override
	public DataType dataType() {
		return dataType;
	}

	@Override
	public boolean deterministic() {
		return deterministic;
	}

	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, "");
	}

	@Override
	public void update(MutableAggregationBuffer buffer, Row row) {
		doBufferCityInfo(buffer, row);
	}

	@Override
	public void merge(MutableAggregationBuffer buffer, Row row) {
		doBufferCityInfo(buffer, row);
	}

	private void doBufferCityInfo(MutableAggregationBuffer buffer, Row row) {
		final String bufferCityInfo = buffer.getString(0);
		String add = Arrays.stream(row.getString(0).split(","))
				.filter(cityInfo -> StringUtils.isNotBlank(cityInfo) && !bufferCityInfo.contains(cityInfo))
				.collect(Collectors.joining(","));
		if (StringUtils.isNotBlank(add)) {
			buffer.update(0, bufferCityInfo + "," + add);
		}
	}

	@Override
	public Object evaluate(Row row) {
		return row.getString(0);
	}
}
