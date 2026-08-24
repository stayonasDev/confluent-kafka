package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerWakeup {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWakeup.class);

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_01");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-01-static");
        //여러 인스턴스를 실행할 때 반드시 static id는 달라야 함
        props.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "3");

        //Default 마지막 offset
//        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        //처음부터 읽지만 이미 __consumer_offsets에 있는 메시지면 처음부터 불가X
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(List.of(topicName));

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
                    log.info("record key:{}, partition:{}, record offset:{}, record value:{}",
                            record.key(), record.partition(), record.offset(), record.value());

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
