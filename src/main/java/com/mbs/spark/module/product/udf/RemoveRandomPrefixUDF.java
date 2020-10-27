package com.mbs.spark.module.product.udf;

import org.apache.spark.sql.api.java.UDF1;

/**
 * 去除随机前缀
 */
public class RemoveRandomPrefixUDF implements UDF1<String, String> {

	private static final long serialVersionUID = 1L;

	@Override
	public String call(String val) throws Exception {
		String[] valSplited = val.split("_");
		return valSplited[1];
	}

}
