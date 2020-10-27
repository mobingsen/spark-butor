package com.mbs.spark.module.session;

import com.google.gson.reflect.TypeToken;
import com.mbs.spark.tools.JsonTool;
import com.mbs.spark.tools.StringUtils;
import org.apache.spark.AccumulatorParam;

import com.mbs.spark.constant.Constants;

import java.util.*;
import java.util.stream.Collectors;

public class SessionAggrStatAccumulator implements AccumulatorParam<String> {

	private static final long serialVersionUID = 6311074555136039130L;

	@Override
	public String zero(String v) {
		return Constants.SESSION_COUNT + "=0|"
				+ Constants.TIME_PERIOD_1s_3s + "=0|"
				+ Constants.TIME_PERIOD_4s_6s + "=0|"
				+ Constants.TIME_PERIOD_7s_9s + "=0|"
				+ Constants.TIME_PERIOD_10s_30s + "=0|"
				+ Constants.TIME_PERIOD_30s_60s + "=0|"
				+ Constants.TIME_PERIOD_1m_3m + "=0|"
				+ Constants.TIME_PERIOD_3m_10m + "=0|"
				+ Constants.TIME_PERIOD_10m_30m + "=0|"
				+ Constants.TIME_PERIOD_30m + "=0|"
				+ Constants.STEP_PERIOD_1_3 + "=0|"
				+ Constants.STEP_PERIOD_4_6 + "=0|"
				+ Constants.STEP_PERIOD_7_9 + "=0|"
				+ Constants.STEP_PERIOD_10_30 + "=0|"
				+ Constants.STEP_PERIOD_30_60 + "=0|"
				+ Constants.STEP_PERIOD_60 + "=0";
	}

	@Override
	public String addInPlace(String v1, String v2) {
		return add(v1, v2);
	}

	@Override
	public String addAccumulator(String v1, String v2) {
		return add(v1, v2);
	}

	/**
	 * session统计计算逻辑
	 * @param v1 连接串
	 * @param v2 范围区间
	 * @return 更新以后的连接串
	 */
	private String add(String v1, String v2) {
		String[] split = v1.split("\\|");
		List<String> list = new ArrayList<>(split.length);
		for (String w : split) {
			String[] arr = w.split("=");
			if (arr[0].equals(v2)) {
				list.add(arr[0] + "=" + Integer.parseInt(arr[1]) + 1);
			} else list.add(w);
		}
		return String.join("|", list);
	}

}
