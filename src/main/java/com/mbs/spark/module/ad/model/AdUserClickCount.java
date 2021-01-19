package com.mbs.spark.module.ad.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.TableGenerator;

/**
 * 用户广告点击量
 */
@Getter
@Setter
@Entity(name = "tb_ad_user_click_count")
public class AdUserClickCount {

	@Id
	@TableGenerator(name = "IdGen", table = "tb_gen", allocationSize = 1)
	@GeneratedValue(generator = "IdGen")
	private Long id;
	private String date;
	private long userId;
	private long adId;
	private long clickCount;
}
