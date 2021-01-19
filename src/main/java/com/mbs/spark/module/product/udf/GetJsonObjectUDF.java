package com.mbs.spark.module.product.udf;

import org.apache.spark.sql.api.java.UDF2;

public class GetJsonObjectUDF implements UDF2<String, String, String> {

	private static final long serialVersionUID = 1L;

	@Override
	public String call(String json, String field) throws Exception {
		try {
//			JSONObject jsonObject = JSONObject.parseObject(json);
//			return jsonObject.getString(field);
			return "";
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
