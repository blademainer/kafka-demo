package com.xiongyingqi.queue;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;

/**
 * @author xiongyingqi
 * @since 16-11-11 下午3:13
 */
public class KafkaQueue {
    private static final Logger logger = LoggerFactory.getLogger(KafkaQueue.class);
    public static final long DEFAULT_TIMEOUT_MILLS = 5000;
    public static final long DEFAULT_READ_TIMEOUT_MILLS = 1000;
    private long timeoutMills = DEFAULT_TIMEOUT_MILLS;
    private long readTimeoutMills = DEFAULT_READ_TIMEOUT_MILLS;
    private String topic = "test";
    private String brokers = "127.0.0.1:9092";
    private String groupId = "test";

    private final ThreadLocal<Producer<String, String>> producer = new ThreadLocal<Producer<String, String>>(){
        @Override
        protected Producer<String, String> initialValue() {
            return createProducer();
        }
    };

    // Kafka consumer
    private final ThreadLocal<Consumer<String, String>> consumer = new ThreadLocal<Consumer<String, String>>() {
        @Override
        protected Consumer<String, String> initialValue() {
            Consumer<String, String> consumer = createConsumer();
            logger.info("Initialized kafka consumer: {}", consumer);
            return consumer;
        }
    };

//    public void start(){
//        producer.set(createProducer());
//    }


    public boolean push(String message) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                topic, message, message);
        Future<RecordMetadata> future = producer.get().send(record);
        if (future == null) {
            return false;
        }
        return true;
    }


    /**
     * 获取下一条数据
     *
     * @return
     */
    public ConsumerRecords<String, String> pull() {
        ConsumerRecords<String, String> records = null;
        try {
            records = consumer.get().poll(readTimeoutMills);
        } catch (Exception e) {
            logger.error("Pull from queue error. Error message: " + e.getMessage(), e);
        }
        if (records == null || records.isEmpty()) {
            return null;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Succeed pull data from queue: {}", records);
        }
        return records;
    }


    private Consumer<String, String> createConsumer() {
        Map<String, Object> consumerProps = new HashMap<String, Object>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        consumerProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1024");
        consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        //TODO
        //consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        logger.info("Initializing kafka consumer with properties: {}", consumerProps);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProps);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    private Producer<String, String> createProducer() {
        // Zookeeper connection properties
        Map<String, Object> producerProps = new HashMap<String, Object>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "10000");
//        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "1024");

        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, readTimeoutMills + "");
        logger.info("Initializing kafka producer with properties: {}", producerProps);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);
        return producer;
    }


    public Producer<String, String> getProducer() {
        return producer.get();
    }


    public Consumer<String, String> getConsumer() {
        return consumer.get();
    }

    public void setConsumer(Consumer<String, String> consumer) {
        this.consumer.set(consumer);
    }

    public long getTimeoutMills() {
        return timeoutMills;
    }

    public void setTimeoutMills(long timeoutMills) {
        this.timeoutMills = timeoutMills;
    }

    public long getReadTimeoutMills() {
        return readTimeoutMills;
    }

    public void setReadTimeoutMills(long readTimeoutMills) {
        this.readTimeoutMills = readTimeoutMills;
    }

}
