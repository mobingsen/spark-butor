package com.mbs.spark.module.task.controller;

import com.mbs.spark.module.task.model.Task;
import com.mbs.spark.module.task.service.TaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

@Controller
@RequestMapping("/t")
public class TaskController {

    private TaskService taskService;

    @Autowired
    public TaskController(TaskService taskService) {
        this.taskService = taskService;
    }

    @RequestMapping("add")
    @ResponseBody
    public boolean add() {
        taskService.add();
        return true;
    }

    @RequestMapping("all")
    @ResponseBody
    public List<Task> all() {
        return taskService.all();
    }
}
