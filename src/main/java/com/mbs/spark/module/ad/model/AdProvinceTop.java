package com.mbs.spark.module.ad.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.TableGenerator;

/**
 * 各省top3热门广告
 */
@Getter
@Setter
@Entity(name = "tb_ad_province_top")
public class AdProvinceTop {

	@Id
	@TableGenerator(name = "IdGen", table = "tb_gen", allocationSize = 1)
	@GeneratedValue(generator = "IdGen")
	private Long id;
	private String date;
	private String province;
	private long adId;
	private long clickCount;
}
