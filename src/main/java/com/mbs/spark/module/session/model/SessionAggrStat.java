package com.mbs.spark.module.session.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.TableGenerator;

/**
 * session聚合统计
 */
@Getter
@Setter
@Entity(name = "tb_session_aggr_stat")
public class SessionAggrStat {

	@Id
	@TableGenerator(name = "IdGen", table = "tb_gen", allocationSize = 1)
	@GeneratedValue(generator = "IdGen")
	private Long id;
	private long taskId;
	private long sessionCount;
	private String result = "";
}
