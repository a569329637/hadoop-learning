package com.gsq.hadoop.hbase.helper;

import com.gsq.hadoop.hbase.domain.HBaseModel;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

@Slf4j
@Service
public class HBaseClientHelper {

    @Resource
    private Connection connection;

    @SneakyThrows
    public void createTable(String tableNameStr, String family) {
        log.info("开始创建表：tableNameStr={}, family={}", tableNameStr, family);
        TableName tableName = TableName.valueOf(tableNameStr);

        Admin admin = connection.getAdmin();
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
        hColumnDescriptor.setMaxVersions(5);
        hColumnDescriptor.setBlockCacheEnabled(true);
        hColumnDescriptor.setBlocksize(180000);
        hTableDescriptor.addFamily(hColumnDescriptor);

        admin.createTable(hTableDescriptor);
        log.info("表创建结束：tableNameStr={}, family={}", tableNameStr, family);
    }

    @SneakyThrows
    public void insert(String tableNameStr, HBaseModel model) {
        TableName tableName = TableName.valueOf(tableNameStr);
        Table table = connection.getTable(tableName);

        Put put = new Put(model.getId().getBytes());
        put.addColumn(model.getFamily().getBytes(), model.getQualifier().getBytes(), model.getValue().getBytes());
        table.put(put);
    }

    @SneakyThrows
    public ResultScanner query(String tableNameStr, String id) {
        TableName tableName = TableName.valueOf(tableNameStr);
        Table table = connection.getTable(tableName);

        Scan scan = new Scan();
        RowFilter rf = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(id));
        scan.setFilter(rf);
        return table.getScanner(scan);
    }

    @SneakyThrows
    public void delete(String tableNameStr, String id) {
        TableName tableName = TableName.valueOf(tableNameStr);
        Table table = connection.getTable(tableName);

        Delete delete = new Delete(id.getBytes());
        table.delete(delete);
    }
}
