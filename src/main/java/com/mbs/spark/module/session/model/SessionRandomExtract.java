package com.mbs.spark.module.session.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Entity;

/**
 * 随机抽取的session
 */
@Getter
@Setter
@Entity
public class SessionRandomExtract {

	private long taskId;
	private String sessionId;
	private String startTime;
	private String searchKeywords;
	private String clickCategoryIds;
}
