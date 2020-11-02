package com.mbs.spark.module.task;

import com.google.gson.Gson;
import com.mbs.spark.converts.JsonConvert;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.util.HashMap;
import java.util.Map;

/**
 * 任务
 */
@Getter
@Setter
@Entity
public class Task {

    @Id
    @TableGenerator(name = "IdGen", table = "tb_gen", allocationSize = 1)
    @GeneratedValue(generator = "IdGen")
    @Column(name = "id")
    private long taskId;
    private String taskName;
    private String createTime;
    private String startTime;
    private String finishTime;
    private String taskType;
    private String taskStatus;
    @Convert(converter = JsonConvert.class)
    @Column(columnDefinition = "TEXT")
    private Map<String, String> args = new HashMap<>();
    private String taskParam;

    public Param toParam() {
        return new Gson().fromJson(this.getTaskParam(), Param.class);
    }
}
