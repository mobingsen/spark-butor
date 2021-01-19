package com.mbs.spark.module.ad.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.TableGenerator;

/**
 * 广告点击趋势
 */
@Getter
@Setter
@Entity(name = "tb_ad_click_trend")
public class AdClickTrend {

	@Id
	@TableGenerator(name = "IdGen", table = "tb_gen", allocationSize = 1)
	@GeneratedValue(generator = "IdGen")
	private Long id;
	private String date;
	private String hour;
	private String minute;
	private long adId;
	private long clickCount;
}
