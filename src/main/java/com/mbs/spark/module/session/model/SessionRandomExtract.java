package com.mbs.spark.module.session.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.TableGenerator;

/**
 * 随机抽取的session
 */
@Getter
@Setter
@Entity
public class SessionRandomExtract {

	@Id
	@TableGenerator(name = "IdGen", table = "tb_gen", allocationSize = 1)
	@GeneratedValue(generator = "IdGen")
	@Column(name = "id")
	private Long id;
	private long taskId;
	private String sessionId;
	private String startTime;
	private String searchKeywords;
	private String clickCategoryIds;
}
