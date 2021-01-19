package com.mbs.spark.module.session.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.TableGenerator;

/**
 * top10活跃session
 */
@Getter
@Setter
@Entity(name = "tb_top_session")
public class TopSession {

	@Id
	@TableGenerator(name = "IdGen", table = "tb_gen", allocationSize = 1)
	@GeneratedValue(generator = "IdGen")
	private Long id;
	private long taskId;
	private long categoryId;
	private String sessionId;
	private long clickCount;
}
