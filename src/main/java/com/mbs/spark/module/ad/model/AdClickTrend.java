package com.mbs.spark.module.ad.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Entity;

/**
 * 广告点击趋势
 */
@Getter
@Setter
@Entity
public class AdClickTrend {

	private String date;
	private String hour;
	private String minute;
	private long adId;
	private long clickCount;
}
