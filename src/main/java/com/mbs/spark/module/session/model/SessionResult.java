package com.mbs.spark.module.session.model;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by 小墨 on 2020/10/17 14:42
 */
@Data
public class SessionResult {

    private long sessionCount;
    private Map<String, Long> timePeriodMap = new HashMap<>();
    private Map<String, Long> stepPeriodMap = new HashMap<>();
/*    private long timePeriod_1s_3s;
    private long timePeriod_4s_6s;
    private long timePeriod_7s_9s;
    private long timePeriod_10s_30s;
    private long timePeriod_30s_60s;
    private long timePeriod_1m_3m;
    private long timePeriod_3m_10m;
    private long timePeriod_10m_30m;
    private long timePeriod_30m;
    private long stepPeriod_1_3;
    private long stepPeriod_4_6;
    private long stepPeriod_7_9;
    private long stepPeriod_10_30;
    private long stepPeriod_30_60;
    private long stepPeriod_60;*/
}
