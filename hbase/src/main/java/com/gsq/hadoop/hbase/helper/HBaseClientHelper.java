package com.gsq.hadoop.hbase.helper;

import com.gsq.hadoop.hbase.domain.HBaseModel;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class HBaseClientHelper {

    @Resource
    private Configuration Configuration;

    @Resource
    private Connection connection;

    @SneakyThrows
    public boolean isTableExist1(String tableName) {
        //在 HBase 中管理、访问表需要先创建 HBaseAdmin 对象
        //Connection connection = ConnectionFactory.createConnection(conf);
        //HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        HBaseAdmin admin = new HBaseAdmin(Configuration);
        return admin.tableExists(tableName);
    }

    @SneakyThrows
    public boolean isTableExist2(String tableName) {
        //在 HBase 中管理、访问表需要先创建 HBaseAdmin 对象
        //Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        return admin.tableExists(tableName);
    }

    @SneakyThrows
    public void createTable(String tableNameStr, String... columnFamily) {
        log.info("开始创建表：tableNameStr={}, columnFamily={}", tableNameStr, columnFamily);
        TableName tableName = TableName.valueOf(tableNameStr);

        Admin admin = connection.getAdmin();
        if (admin.tableExists(tableName)) {
            log.info("开始创建表：表{}已经存在，现在先删除", tableNameStr);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        for (String cf : columnFamily) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hColumnDescriptor.setMaxVersions(5);
            hColumnDescriptor.setBlockCacheEnabled(true);
            hColumnDescriptor.setBlocksize(180000);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        admin.createTable(hTableDescriptor);
        log.info("表创建结束：tableNameStr={}, columnFamily={}", tableNameStr, columnFamily);
    }

    @SneakyThrows
    public void dropTable(String tableNameStr) {
        HBaseAdmin admin = new HBaseAdmin(Configuration);
        if(isTableExist1(tableNameStr)){
            admin.disableTable(tableNameStr);
            admin.deleteTable(tableNameStr);
            System.out.println("表" + tableNameStr + "删除成功！");
        }else{
            System.out.println("表" + tableNameStr + "不存在！");
        }
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
    public ResultScanner queryAll(String tableNameStr) {
        TableName tableName = TableName.valueOf(tableNameStr);
        Table table = connection.getTable(tableName);

        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner){
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                //得到 row key
                System.out.println(" 行 键 :" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println(" 列 族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(" 列 :" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(" 值 :" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        return resultScanner;
    }

    @SneakyThrows
    public void delete(String tableNameStr, String... ids) {
        TableName tableName = TableName.valueOf(tableNameStr);
        Table table = connection.getTable(tableName);

        List<Delete> deleteList = new ArrayList<>();
        for (String id : ids) {
            Delete delete = new Delete(id.getBytes());
            deleteList.add(delete);
        }
        table.delete(deleteList);
    }
}
