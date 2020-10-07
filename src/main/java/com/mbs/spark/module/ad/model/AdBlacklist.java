package com.mbs.spark.module.ad.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;

/**
 * 广告黑名单
 */
@Getter
@Setter
@Entity
public class AdBlacklist {

	@Column
	private long userId;
}
