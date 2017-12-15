package com.xiongyingqi.server;

import com.xiongyingqi.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.util.FileSystemUtils;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * @author xiongyingqi
 * @since 2016-11-19 00:10
 */
//@Profile("local")
@Component
public class LocalEnvStartup implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(LocalEnvStartup.class);
    @Autowired
    private KafkaConfig kafkaConfig;
    KafkaLocal kafkaLocal;

    private void setUpKafka() throws IOException, InterruptedException {
        Properties zooProperties = new Properties();
        Properties properties = new Properties();
        properties.load(getClass().getClassLoader().getResourceAsStream("server.properties"));
//        properties.load(kafkaProperties.getProperties());
        zooProperties.load(getClass().getClassLoader().getResourceAsStream("zookeeper.properties"));

        // delete dataDir
        String dataDir = zooProperties.getProperty("dataDir");
        File file = new File(dataDir);
        if (file.exists()) {
            FileSystemUtils.deleteRecursively(file);
        }

        String kafkaDir = kafkaConfig.getLogDir();
        File kafkaDirFile = new File(kafkaDir);
        if (kafkaDirFile.exists()) {
            FileSystemUtils.deleteRecursively(kafkaDirFile);
        }

        kafkaLocal = new KafkaLocal(properties, zooProperties);
        kafkaLocal.kafka.startup();

//        kafkaLocal.createTopic("127.0.0.1:" + zooProperties.getProperty("clientPort"), "codeMessageQueue");
//        kafkaLocal.createTopic("127.0.0.1:" + zooProperties.getProperty("clientPort"), "salesMessageQueue");

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                try {
                    kafkaLocal.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    File file = new File("./tmp");
                    System.out.println("deleting..." + file.getAbsolutePath());
                    FileSystemUtils.deleteRecursively(file);
                    System.out.println("deleted..." + file.getAbsolutePath());
                }
            }
        });

    }

    @Override
    public void afterPropertiesSet() throws Exception {
        try {
            setUpKafka();
        } catch (Exception e) {
            logger.error("", e);
        }
//        try {
//            setUpRedis();
//        } catch (Exception e) {
//            logger.error("", e);
//        }
    }
}
