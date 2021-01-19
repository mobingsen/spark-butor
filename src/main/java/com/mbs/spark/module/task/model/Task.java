package com.mbs.spark.module.task.model;

import com.mbs.spark.converts.JsonConvert;
import com.mbs.spark.module.task.domain.Param;
import com.mbs.spark.utils.JsonUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.persistence.*;
import java.util.HashMap;
import java.util.Map;

/**
 * 任务
 */
@Getter
@Setter
@Accessors(chain = true)
@Entity(name = "tb_task")
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
        return JsonUtil.from(this.getTaskParam(), Param.class);
    }
}
