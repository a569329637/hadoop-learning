package com.gsq.hadoop.hbase.service;

import com.google.common.collect.Lists;
import com.gsq.hadoop.hbase.domain.HBaseModel;
import com.gsq.hadoop.hbase.helper.HBaseClientHelper;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.List;

@Service
public class HBaseClientServiceImpl implements HBaseService {

    @Autowired
    private HBaseClientHelper hBaseClientHelper;

    @Override
    public void createTable(String tableName, String family) {
        hBaseClientHelper.createTable(tableName, family);
    }

    @Override
    public void insert(String tableName, HBaseModel model) {
        hBaseClientHelper.insert(tableName, model);
    }

    @Override
    public List<HBaseModel> query(String tableName, String family, String qualifier, String id) {
        ResultScanner rs = hBaseClientHelper.query(tableName, id);

        List<HBaseModel> hBaseModelList = Lists.newArrayList();
        Iterator<Result> iterator = rs.iterator();
        while(iterator.hasNext()) {
            Result result = iterator.next();
            byte[] rowKey = result.getRow();
            byte[] value = result.getValue(family.getBytes(), qualifier.getBytes());
            HBaseModel hBaseModel = new HBaseModel()
                    .setId(new String(rowKey))
                    .setFamily(family)
                    .setQualifier(qualifier)
                    .setValue(new String(value));
            hBaseModelList.add(hBaseModel);
        }
        return hBaseModelList;
    }

    @Override
    public void delete(String tableName, String id) {
        hBaseClientHelper.delete(tableName, id);
    }
}
