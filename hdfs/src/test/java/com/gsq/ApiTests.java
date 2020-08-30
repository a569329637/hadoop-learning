package com.gsq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class ApiTests {

    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://songze001:9000"), configuration, "root");

        // 2 上传文件
        fs.copyFromLocalFile(new Path("F:/hadoop_test/banzhang.txt"), new Path("/banzhang.txt"));

        // 3 关闭资源
        fs.close();

        System.out.println("over");
    }

    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://songze001:9000"), configuration, "root");

        fs.copyToLocalFile(new Path("/banzhang.txt"), new Path("F:/hadoop_test/banzhang1.txt"));

        fs.close();

        System.out.println("over");
    }

    @Test
    public void testDelete() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://songze001:9000"), configuration, "root");

        fs.delete(new Path("/root/wcoutput"), true);

        fs.close();

        System.out.println("over");
    }

    @Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://songze001:9000"), configuration, "root");

        fs.rename(new Path("/banzhang.txt"), new Path("/banhua.txt"));

        fs.close();

        System.out.println("over");
    }

    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException{

        // 1获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://songze001:9000"), configuration, "root");

        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();

            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());

            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {

                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();

                for (String host : hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("-----------班长的分割线----------");
        }

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testListStatus() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://songze001:9000"), configuration, "root");

        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {

            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testPutFileToHdfs() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://songze001:9000"), configuration, "root");

        FileInputStream fis = new FileInputStream(new File("F:/hadoop_test/banzhang.txt"));

        FSDataOutputStream fos = fs.create(new Path("/banzhang1.txt"));

        IOUtils.copyBytes(fis, fos, configuration);

        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);

        fs.close();
    }

    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://songze001:9000"), configuration, "root");

        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("F:/hadoop_test/banzhang.txt"));

        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/banzhang2.txt"));

        // 4 流对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://songze001:9000"), configuration, "root");

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/banhua.txt"));

        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("F:/hadoop_test/banhua.txt"));

        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

}
