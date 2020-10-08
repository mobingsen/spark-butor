package com.mbs.spark.module.product.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.TableGenerator;

/**
 * top10品类
 */
@Getter
@Setter
@Entity
public class Top10Category {

	@Id
	@TableGenerator(name = "IdGen", table = "tb_gen", allocationSize = 1)
	@GeneratedValue(generator = "IdGen")
	@Column(name = "id")
	private Long id;
	private long taskId;
	private long categoryId;
	private long clickCount;
	private long orderCount;
	private long payCount;
}
