package com.mbs.spark.module.ad.repository;

import com.mbs.spark.module.ad.model.AdUserClickCount;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

/**
 * @Created by mobingsen on 2020/10/7 22:37
 */
public interface AdUserClickCountRepository extends JpaRepository<AdUserClickCount, Long> {

    /**
     * 根据多个key查询用户广告点击量
     * @param date 日期
     * @param userId 用户id
     * @param adId 广告id
     * @return
     */
    @Query("SELECT acc.clickCount FROM tb_ad_user_click_count acc WHERE acc.date=?1 AND acc.userId=?2 AND acc.adId=?3")
    int findClickCountByMultiKey(String date, long userId, long adId);
}
