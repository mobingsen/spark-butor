package com.mbs.spark.module.ad.model;

import lombok.Getter;
import lombok.Setter;
import scala.Tuple2;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.TableGenerator;

/**
 * 广告实时统计
 */
@Getter
@Setter
@Entity(name = "tb_ad_stat")
public class AdStat {

	@Id
	@TableGenerator(name = "IdGen", table = "tb_gen", allocationSize = 1)
	@GeneratedValue(generator = "IdGen")
	private Long id;
	private String date;
	private String province;
	private String city;
	private long adId;
	private long clickCount;

	public static AdStat ctor(Tuple2<String, Long> tuple) {
		String[] keyArr = tuple._1.split("_");
		String date = keyArr[0];
		String province = keyArr[1];
		String city = keyArr[2];
		long adId = Long.parseLong(keyArr[3]);
		long clickCount = tuple._2;
		AdStat adStat = new AdStat();
		adStat.setDate(date);
		adStat.setProvince(province);
		adStat.setCity(city);
		adStat.setAdId(adId);
		adStat.setClickCount(clickCount);
		return adStat;
	}
}
