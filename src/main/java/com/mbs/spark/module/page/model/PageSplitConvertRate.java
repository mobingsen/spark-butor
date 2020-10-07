package com.mbs.spark.module.page.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Entity;

/**
 * 页面切片转化率
 */
@Getter
@Setter
@Entity
public class PageSplitConvertRate {

	private long taskId;
	private String convertRate;
}
