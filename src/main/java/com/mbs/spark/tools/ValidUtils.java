package com.mbs.spark.tools;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 校验工具类
 */
public class ValidUtils {

	/**
	 * 校验数据中的指定字段，是否在指定范围内
	 * @param data 数据
	 * @param dataField 数据字段
	 * @param parameter 参数
	 * @param startParamField 起始参数字段
	 * @param endParamField 结束参数字段
	 * @return 校验结果
	 */
	public static boolean between(String data, String dataField, String parameter, String startParamField, String endParamField) {
		Map<String, Integer> map = Arrays.stream(parameter.split("\\|"))
				.map(kv -> kv.split("="))
				.collect(Collectors.toMap(arr -> arr[0], arr -> Integer.parseInt(arr[1])));
		if(!map.containsKey(startParamField) || !map.containsKey(endParamField)) {
			return true;
		}
		int startParamFieldValue = map.get(startParamField);
		int endParamFieldValue = map.get(endParamField);
		return Arrays.stream(data.split("\\|"))
				.filter(kv -> kv.startsWith(dataField) && dataField.equals(kv.split("=")[0]))
				.map(kv -> Integer.parseInt(kv.split("=")[1]))
				.findAny()
				.map(dfv -> dfv >= startParamFieldValue && dfv <= endParamFieldValue)
				.orElse(false);
	}

	/**
	 * 校验数据中的指定字段，是否有值与参数字段的值相同
	 * @param data 数据
	 * @param dataField 数据字段
	 * @param parameter 参数
	 * @param paramField 参数字段
	 * @return 校验结果
	 */
	public static boolean in(String data, String dataField,
			String parameter, String paramField) {
		Map<String, String> map = Arrays.stream(parameter.split("\\|"))
				.map(kv -> kv.split("="))
				.collect(Collectors.toMap(arr -> arr[0], arr -> arr[1]));
		String paramFieldValue = map.get(paramField);
		if(paramFieldValue == null) {
			return true;
		}
		String[] paramFieldValueSplited = paramFieldValue.split(",");
		Map<String, String> dataMap = Arrays.stream(data.split("\\|"))
				.map(kv -> kv.split("="))
				.collect(Collectors.toMap(arr -> arr[0], arr -> arr[1]));
		String dataFieldValue = dataMap.get(dataField);
		if(dataFieldValue != null) {
			String[] dataFieldValueSplited = dataFieldValue.split(",");

			for(String singleDataFieldValue : dataFieldValueSplited) {
				for(String singleParamFieldValue : paramFieldValueSplited) {
					if(singleDataFieldValue.equals(singleParamFieldValue)) {
						return true;
					}
				}
			}
 		}

		return false;
	}

	/**
	 * 校验数据中的指定字段，是否在指定范围内
	 * @param data 数据
	 * @param dataField 数据字段
	 * @param parameter 参数
	 * @param paramField 参数字段
	 * @return 校验结果
	 */
	public static boolean equal(String data, String dataField, String parameter, String paramField) {
		Map<String, String> map = Arrays.stream(parameter.split("\\|"))
				.map(kv -> kv.split("="))
				.collect(Collectors.toMap(arr -> arr[0], arr -> arr[1]));
		String paramFieldValue = map.get(paramField);
		if(paramFieldValue == null) {
			return true;
		}
		Map<String, String> dmap = Arrays.stream(data.split("\\|"))
				.map(kv -> kv.split("="))
				.collect(Collectors.toMap(arr -> arr[0], arr -> arr[1]));
		String dataFieldValue = dmap.get(dataField);
		if(dataFieldValue != null) {
			if(dataFieldValue.equals(paramFieldValue)) {
				return true;
			}
 		}

		return false;
	}

}
