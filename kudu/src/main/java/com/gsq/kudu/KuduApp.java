package com.gsq.kudu;


import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.ArrayList;

/**
 * https://www.jianshu.com/p/c73cb4d2db0e
 * https://blog.csdn.net/lvwenyuan_1/article/details/107058980
 */
public class KuduApp {

    private KuduClient kuduClient;
    //指定kuduMaster地址
    private String kuduMaster;

    public KuduApp(String kuduMaster) {
        this.kuduMaster = kuduMaster;
        this.kuduClient = new KuduClient
                .KuduClientBuilder(kuduMaster)
                .build();
    }

    public void createTable(String tableName) throws KuduException {
        if (!kuduClient.tableExists(tableName)) {
            System.out.println("tableName = " + tableName + ", 不存在，现在创建");

            // 构建创建表的schema信息 --- 就是表的字段和类型
            ArrayList<ColumnSchema> columnSchemas = new ArrayList<>();
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).build());
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("score", Type.INT32).build());
            Schema schema = new Schema(columnSchemas);

            CreateTableOptions options = new CreateTableOptions();
            ArrayList<String> partitionList = new ArrayList<>();
            //指定kudu表的分区字段是什么
            partitionList.add("id");
            //按照 id.hashcode % 分区数 = 分区号
            options.addHashPartitions(partitionList, 6);

            kuduClient.createTable(tableName, schema, options);
        } else {
            System.out.println("tableName = " + tableName + ", 已经存在");
        }
    }

    public void scanTable(String tableName) throws KuduException {

        //需要使用kuduTable来构建Operation的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(tableName);

        //创建scanner
        KuduScanner scanner = kuduClient.newScannerBuilder(kuduTable).build();

        //遍历数据
        while (scanner.hasMoreRows()) {
            for (RowResult rowResult : scanner.nextRows()) {
                System.out.println("scan table: >>>>>>>" + rowResult.getInt(0) + "\t" + rowResult.getString(1)
                        + "\t" + rowResult.getInt(2));
            }
        }
    }

    public void insertTable(String tableName) throws KuduException {
        // 向表加载数据需要一个kuduSession对象
        KuduSession kuduSession = kuduClient.newSession();
        // 自动 flush
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

        //需要使用kuduTable来构建Operation的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(tableName);

        for (int i = 1; i <= 10; i++) {
            Insert insert = kuduTable.newInsert();
            PartialRow row = insert.getRow();
            row.addInt("id", i);
            row.addString("name", "zhangsan-" + i);
            row.addInt("score", 20 + i);
            //最后时限执行数据的加载操作
            kuduSession.apply(insert);
        }

        kuduSession.close();
    }

    public void updateTable(String tableName) throws KuduException {
        KuduSession kuduSession = kuduClient.newSession();
        // 手动 flush
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

        KuduTable kuduTable = kuduClient.openTable(tableName);
        Update update = kuduTable.newUpdate();

        PartialRow row = update.getRow();
        row.addInt("id", 1);
        row.addString("name", "lisi-1");
        kuduSession.apply(update);
        // 手动 flush
        kuduSession.flush();
        kuduSession.close();
    }

    public void deleteTable(String tableName) throws KuduException {
        KuduSession kuduSession = kuduClient.newSession();
        //设置手动刷新
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

        KuduTable kuduTable = kuduClient.openTable(tableName);
        Delete delete = kuduTable.newDelete();
        delete.getRow().addInt("id", 5);
        kuduSession.apply(delete);
        kuduSession.flush();
        kuduSession.close();
    }

    public void searchRowWithRange(String tableName) throws KuduException {
        //创建一个数组,并添加相应的表字段
        ArrayList<String> projectColumns = new ArrayList<String>();
        projectColumns.add("id");
        projectColumns.add("name");
        projectColumns.add("score");

        KuduTable kuduTable = kuduClient.openTable(tableName);
        Schema schema = kuduTable.getSchema();

        PartialRow partialRow = schema.newPartialRow();
        partialRow.addInt("id", 1);

        PartialRow partialRow1 = schema.newPartialRow();
        partialRow1.addInt("id", 5);

        KuduScanner scanner = kuduClient.newScannerBuilder(kuduTable)
                .setProjectedColumnNames(projectColumns) //指定输出列
                .lowerBound(partialRow) //指定下限(包含)
                .exclusiveUpperBound(partialRow1) //指定上限(不包含)
                .build();
        while (scanner.hasMoreRows()) {
            for (RowResult rowResult : scanner.nextRows()) {
                System.out.println("range scanner: " + rowResult.getInt(0) + "\t" + rowResult.getString(1)
                        + "\t" + rowResult.getInt(2));
            }
        }
    }

    public void searchWithCondition(String tableName) throws KuduException {
        KuduTable kuduTable = kuduClient.openTable(tableName);
        Schema schema = kuduTable.getSchema();

        //创建一个数组,并添加相应的表字段
        ArrayList<String> projectColumns = new ArrayList<String>();
        projectColumns.add("id");
        projectColumns.add("name");
        projectColumns.add("score");

        //创建predicate
        KuduPredicate kuduPredicate = KuduPredicate
                .newComparisonPredicate(
                        schema.getColumn("id"),
                        KuduPredicate.ComparisonOp.EQUAL, 1);
        KuduScanner kuduScanner = kuduClient.newScannerBuilder(kuduTable)
                .readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT) //设置读取快照模式
                .setProjectedColumnNames(projectColumns) //设置要读取的列
                .addPredicate(kuduPredicate) //设置predicate
                .build();

        while (kuduScanner.hasMoreRows()) {
            for (RowResult rowResult : kuduScanner.nextRows()) {
                System.out.println("range scanner: " + rowResult.getInt(0) + "\t" + rowResult.getString(1)
                        + "\t" + rowResult.getInt(2));
            }
        }
    }

    public static void main(String[] args) throws KuduException {
//        String kuduMaster = "kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251";
        String kuduMaster = "localhost:7051,localhost:7151,localhost:7251";
        String tableName = "kudu_student";

        KuduApp kuduApp = new KuduApp(kuduMaster);
        kuduApp.createTable(tableName);
        System.out.println("================================\n\n");

        kuduApp.scanTable(tableName);
        System.out.println("================================\n\n");

        kuduApp.insertTable(tableName);

        kuduApp.updateTable(tableName);
        kuduApp.scanTable(tableName);
        System.out.println("================================\n\n");

        kuduApp.deleteTable(tableName);
        kuduApp.scanTable(tableName);
        System.out.println("================================\n\n");

        kuduApp.searchRowWithRange(tableName);
        System.out.println("================================\n\n");

        kuduApp.searchWithCondition(tableName);
        System.out.println("================================\n\n");
    }
}
