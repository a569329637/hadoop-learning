package com.gsq.hadoop.hbase.controller;

import com.gsq.hadoop.hbase.domain.HBaseModel;
import com.gsq.hadoop.hbase.service.HBaseService;
import com.gsq.hadoop.hbase.utils.IdUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("hbase-client")
public class HBaseTestController {

    private static final String TABLE_STUDENT = "t_student";
    private static final String CF_FAMILY = "family";
    private static final String Q_NAME = "name";

    @Autowired
    private HBaseService hBaseService;

    @GetMapping("createTable")
    public String createTable() {
        hBaseService.createTable(TABLE_STUDENT, CF_FAMILY);
        return "success";
    }

    @GetMapping("insert")
    public String insert() {
        for (int i = 0; i < 20; i++) {
            String id = IdUtils.generateId();
            HBaseModel hBaseModel = new HBaseModel()
                    .setId(id)
                    .setFamily(CF_FAMILY)
                    .setQualifier(Q_NAME)
                    .setValue("Tom" + i);
            hBaseService.insert(TABLE_STUDENT, hBaseModel);
        }
        return "success";
    }

    @GetMapping("query")
    public List<HBaseModel> query() {
        String id = "xxx";
        return hBaseService.query(TABLE_STUDENT, CF_FAMILY, Q_NAME, id);
    }

    @GetMapping("delete")
    public void delete() {
        String id = "xxx";
        hBaseService.delete(TABLE_STUDENT, id);
    }

}
