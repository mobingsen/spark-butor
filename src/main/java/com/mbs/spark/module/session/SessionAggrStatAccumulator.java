package com.mbs.spark.module.session;

import com.mbs.spark.constant.Constants;
import org.apache.spark.AccumulatorParam;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SessionAggrStatAccumulator implements AccumulatorParam<String> {

	private static final long serialVersionUID = 6311074555136039130L;

	@Override
	public String zero(String v) {
		return Stream.of(
				Constants.SESSION_COUNT,
				Constants._1s_3s,
				Constants._4s_6s,
				Constants._7s_9s,
				Constants._10s_30s,
				Constants._30s_60s,
				Constants._1m_3m,
				Constants._3m_10m,
				Constants._10m_30m,
				Constants._30m,
				Constants._1_3,
				Constants._4_6,
				Constants._7_9,
				Constants._10_30,
				Constants._30_60,
				Constants._60
		).map(k -> k + "=0").collect(Collectors.joining("|"));
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
