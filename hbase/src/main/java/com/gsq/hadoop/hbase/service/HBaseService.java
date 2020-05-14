package com.gsq.hadoop.hbase.service;

import com.gsq.hadoop.hbase.domain.HBaseModel;

import java.util.List;

public interface HBaseService {

    void createTable(String tableName, String family);

    void insert(String tableName, HBaseModel model);

    List<HBaseModel> query(String tableName, String family, String qualifier, String id);

    void delete(String tableName, String id);

}
