package com.xiongyingqi.server;

import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * @author xiongyingqi
 * @since 2016-11-12 18:29
 */
public class KafkaLocal {
    public KafkaServerStartable kafka;
    public ZooKeeperLocal zookeeper;

    public KafkaLocal(Properties kafkaProperties, Properties zkProperties) throws IOException, InterruptedException {
        //start local zookeeper
        System.out.println("starting local zookeeper...");
        zookeeper = new ZooKeeperLocal(zkProperties);
        System.out.println("zookeeper done");

        //start local kafka broker
//        kafka = new KafkaServerStartable(kafkaConfig);
        kafka = KafkaServerStartable.fromProps(kafkaProperties);
        System.out.println("starting local kafka broker...");
        kafka.startup();
        System.out.println("kafka done");
    }

    public KafkaLocal(Map<String, String> kafkaProperties, Properties zkProperties) throws IOException, InterruptedException {
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

        //start local zookeeper
        System.out.println("starting local zookeeper...");
        zookeeper = new ZooKeeperLocal(zkProperties);
        System.out.println("zookeeper done");

        //start local kafka broker
        kafka = new KafkaServerStartable(kafkaConfig);
        System.out.println("starting local kafka broker...");
        kafka.startup();
        System.out.println("kafka done");
    }

    public void createTopic(String zkUrl, String topic){

        ZkUtils zkUtils = ZkUtils.apply(zkUrl, 10000, 10000, false);
        AdminUtils.createTopic(zkUtils, topic, 3, 1, new Properties(), null);

    }


    public void stop() {
        //stop kafka broker
        System.out.println("stopping kafka...");
        kafka.shutdown();
        zookeeper.stop();
        System.out.println("done");
    }

}
