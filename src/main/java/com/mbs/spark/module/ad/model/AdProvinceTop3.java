package com.mbs.spark.module.ad.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Entity;

/**
 * 各省top3热门广告
 */
@Getter
@Setter
@Entity
public class AdProvinceTop3 {

	private String date;
	private String province;
	private long adId;
	private long clickCount;
}
