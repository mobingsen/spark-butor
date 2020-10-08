package com.mbs.spark.module.task.model;

import com.google.gson.Gson;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.TableGenerator;
import java.io.Serializable;

/**
 * 任务
 */
@Getter
@Setter
@Entity
public class Task {

	@Id
	@TableGenerator(name = "IdGen", table = "tb_gen", allocationSize = 1)
	@GeneratedValue(generator = "IdGen")
	@Column(name = "id")
	private long taskId;
	private String taskName;
	private String createTime;
	private String startTime;
	private String finishTime;
	private String taskType;
	private String taskStatus;
	private String taskParam;

	public Param toParam() {
		return new Gson().fromJson(this.getTaskParam(), Param.class);
	}
}
