package com.keli.platform.compact.source;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerClient {
    public static void main(String[] args) {
        Config conf = ConfigFactory.load("default.properties");

        Properties properties = new Properties();

        // Kafka集群的地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getString("kafka.bootstrap.servers"));
        // 消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, conf.getString("kafka.group.id"));

        // 开启自动提交 offset 功能
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 自动提交 offset 的时间间隔
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        /*
         * auto.offset.reset
         * 重置消费者的offset
         * 该参数生效的两种情况：
         * 1、换了一个新的消费者组
         * 2、没换消费者组，之前消费者组消费过数据，但是消费过的offset在集群中已经不存在了。
         *    最常见的情况：过了七天，这些数据已经被kafka删除了
         * 两个参数说明：
         * 1、earliest：从最早的数据开始消费。
         * 2、latest(默认值)：从大的offset开始消费(从最新的数据开始消费)，以前消费的数据消费不到。
         */
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // key.deserializer
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // value.deserializer
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 订阅主题
        consumer.subscribe(Collections.singletonList(conf.getString("kafka.topic")));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}
