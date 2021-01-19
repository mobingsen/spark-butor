package com.mbs.spark.module.task.domain;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Created by 小墨 on 2020/10/8 0:11
 */
@Getter
@Setter
@Accessors(chain = true)
public class Param {

    private String startDate;
    private String endDate;
    private String targetPageFlow;
    private String startAge;
    private String endAge;
    private String professionals;
    private String cities;
    private String sex;
    private String keywords;
    private String categoryIds;
}
