package com.mbs.spark.module.task.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Entity;
import java.io.Serializable;

/**
 * 任务
 */
@Getter
@Setter
@Entity
public class Task implements Serializable {

	private static final long serialVersionUID = 3518776796426921776L;

	private long taskId;
	private String taskName;
	private String createTime;
	private String startTime;
	private String finishTime;
	private String taskType;
	private String taskStatus;
	private String taskParam;
}
