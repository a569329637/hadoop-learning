package com.gsq.hadoop.hbase.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

@org.springframework.context.annotation.Configuration
public class HBaseClientConfig {

    @Value("${hbase.zk.host}")
    private String hbaseZkHost;
    @Value("${hbase.zk.port}")
    private String hbaseZkPort;

    @Autowired
    private Configuration configuration;

    @Bean
    public Configuration getConfiguration() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", hbaseZkHost);
        configuration.set("hbase.zookeeper.property.clientPort", hbaseZkPort);
//        configuration.set("hbase.regionserver.port", "16201");
        return configuration;
    }

    @Bean
    public Connection getConnection() throws IOException {
        return ConnectionFactory.createConnection(configuration);
    }

}
