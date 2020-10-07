package com.mbs.spark.module.product.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Entity;

/**
 * top10品类
 */
@Getter
@Setter
@Entity
public class Top10Category {

	private long taskId;
	private long categoryId;
	private long clickCount;
	private long orderCount;
	private long payCount;
}
