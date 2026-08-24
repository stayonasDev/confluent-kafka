package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerWakeupMTopicRebalance {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWakeupMTopicRebalance.class);

    public static void main(String[] args) {

//        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-assign");
//        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(List.of("topic-p3-t1", "topic-p3-t2"));

        //main thread
        Thread mainThread = Thread.currentThread();

        //메인 스레드가 죽기 전에 훅 이벤트
        //Main Thread 종료 시 별도의 Thread로 KafkaConsumer wakeup() 메소드를 호출하게 함
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("main program starts to exit by calling wakeup");
                //while 문에서 빠져나가기 위한 poll 기능에 wakeup (연결 종료를 위해)
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord record : consumerRecords) {
                    log.info("topic:{}, record key:{}, partition:{}, record offset:{}, record value:{}",
                            record.topic(), record.key(), record.partition(), record.offset(), record.value());

                }
            }
        } catch (WakeupException e) {
            log.error("wakeup exception has been called");
        } finally {
            log.info("finally consumer is closing");
            consumer.close();
        }

    }
}
