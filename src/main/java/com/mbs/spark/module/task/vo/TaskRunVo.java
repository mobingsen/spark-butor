package com.mbs.spark.module.task.vo;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

/**
 * @Created by mobingsen on 2021/1/19 17:51
 */
@Data
@Accessors(chain = true)
public class TaskRunVo {

    private String taskName;
    private String type;
    private Map<String, String> args = new HashMap<>();
}
