package com.xiongyingqi;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xiongyingqi.config.KafkaConfig;
import com.xiongyingqi.queue.KafkaQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;

import java.util.Iterator;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@EnableConfigurationProperties(KafkaConfig.class)
@SpringBootApplication
public class KafkaLingerDemoApplication {
    // Thread pool
    private static ExecutorService executorService = new ThreadPoolExecutor(10, 10, 1000,
            TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactoryBuilder()
            .setNameFormat("kafka-queue-thread-%d").build());

    public static void main(String[] args) {
        SpringApplication.run(KafkaLingerDemoApplication.class, args);
        test();
    }

    public static void test() {
        KafkaQueue kafkaQueue = new KafkaQueue();

        for (int i = 0; i < 10; i++) {
            new Thread() {
                @Override
                public void run() {
                    while (!Thread.interrupted()) {
                        ConsumerRecords<String, String> pull = kafkaQueue.pull();
                        if (pull == null) {
                            continue;
                        }
                        pull.forEach(x -> {
                            String value = x.value();
                            Long time = Long.parseLong(value);
                            Long useTime = System.currentTimeMillis() - time;
                            System.err.println("Thread: " + Thread.currentThread().getName() + "consume value === " + value + " use time: " + useTime);
                        });
                    }
                }
            }.start();
        }


        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String next = scanner.next();
            int i1 = 0;
            try {
                i1 = Integer.parseInt(next.trim());
                for (int i = 0; i < i1; i++) {
                    int finalI = i;
                    new Thread(() -> {
                        executorService.execute(new Runnable() {
                            @Override
                            public void run() {
                                long l = System.currentTimeMillis();
                                kafkaQueue.push(l + "");
                                System.out.println("sent: " + l);
                            }
                        });
                    }).start();
                }
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }
    }
}
