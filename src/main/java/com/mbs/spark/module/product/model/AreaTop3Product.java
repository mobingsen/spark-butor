package com.mbs.spark.module.product.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Row;

import javax.persistence.Entity;

/**
 * 各区域top3热门商品
 */
@Getter
@Setter
@Entity
public class AreaTop3Product {

	private long taskId;
	private String area;
	private String areaLevel;
	private long productId;
	private String cityInfos;
	private long clickCount;
	private String productName;
	private String productStatus;

	public static AreaTop3Product ctor(long taskId, Row row) {
		AreaTop3Product product = new AreaTop3Product();
		product.setTaskId(taskId);
		product.setArea(row.getString(0));
		product.setAreaLevel(row.getString(1));
		product.setProductId(row.getLong(2));
		product.setClickCount(Long.parseLong(String.valueOf(row.get(3))));
		product.setCityInfos(row.getString(4));
		product.setProductName(row.getString(5));
		product.setProductStatus(row.getString(6));
		return product;
	}
}
