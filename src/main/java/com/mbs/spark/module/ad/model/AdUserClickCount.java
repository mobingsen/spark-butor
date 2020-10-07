package com.mbs.spark.module.ad.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Entity;

/**
 * 用户广告点击量
 */
@Getter
@Setter
@Entity
public class AdUserClickCount {

	private String date;
	private long userId;
	private long adId;
	private long clickCount;
}
