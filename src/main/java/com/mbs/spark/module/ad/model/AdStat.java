package com.mbs.spark.module.ad.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Entity;

/**
 * 广告实时统计
 */
@Getter
@Setter
@Entity
public class AdStat {

	private String date;
	private String province;
	private String city;
	private long adId;
	private long clickCount;
}
