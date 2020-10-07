package com.mbs.spark.module.session.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Entity;

/**
 * top10活跃session
 */
@Getter
@Setter
@Entity
public class Top10Session {

	private long taskId;
	private long categoryId;
	private String sessionId;
	private long clickCount;
}
