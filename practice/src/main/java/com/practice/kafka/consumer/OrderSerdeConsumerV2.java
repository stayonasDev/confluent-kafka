package com.practice.kafka.consumer;

import com.practice.kafka.model.OrderModel;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class OrderSerdeConsumerV2{
    public static final Logger log = LoggerFactory.getLogger(OrderSerdeConsumerV2.class.getName());

    private KafkaConsumer<String, OrderModel> kafkaConsumer;
    private List<java.lang.String> topics;

    public OrderSerdeConsumerV2(Properties consumerProps, List<java.lang.String> topics) {
        this.kafkaConsumer = new KafkaConsumer<String, OrderModel>(consumerProps);
        this.topics = topics;
    }

    public void initConsumer() {
        this.kafkaConsumer.subscribe(this.topics);
        shutdownHookToRuntime(this.kafkaConsumer);
    }

    private void shutdownHookToRuntime(KafkaConsumer<String, OrderModel> kafkaConsumer) {
        //main thread
        Thread mainThread = Thread.currentThread();

        //main thread 종료시 별도의 thread로 KafkaConsumer wakeup()메소드를 호출하게 함.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info(" main program starts to exit by calling wakeup");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch(InterruptedException e) { e.printStackTrace();}
            }
        });
    }

    private void processRecord(ConsumerRecord<String, OrderModel> record) {
        log.info("record key: {}, partition: {}, offset: {} record value: {}",
                record.key(), record.partition(), record.offset(), record.value());
    }

    private void processRecords(ConsumerRecords<String, OrderModel> records) {
//        records.forEach(record -> processRecord(record));
        records.forEach(this::processRecord);
    }

    private void pollCommitAsync(long durationMillis) throws WakeupException , Exception {
        ConsumerRecords<String, OrderModel> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);
        this.kafkaConsumer.commitAsync((offsets, exception) -> {
            if (exception != null) {
                log.info("offsets {} is not completed, error:{}", offsets, exception.getMessage());
            }
        });
    }

    public void pollConsumes(long durationMillis, java.lang.String commitMode) {
        try {
            while (true) {
                if (commitMode.equals("sync")) {
                    pollCommitSync(durationMillis);
                } else {
                    pollCommitAsync(durationMillis);
                }
            }
        }catch(WakeupException e) {
            log.error("wakeup exception has been called");
        }catch(Exception e) {
            log.error(e.getMessage());
        }finally {
            log.info("##### commit sync before closing");
            kafkaConsumer.commitSync();
            log.info("finally consumer is closing");
            closeConsumer();
        }
    }


    private void pollCommitSync(long durationMillis) throws WakeupException, Exception {
        ConsumerRecords<String, OrderModel> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);
        try {
            if(consumerRecords.count() > 0 ) {
                this.kafkaConsumer.commitSync();
                log.info("commit sync has been called");
            }
        } catch(CommitFailedException e) {
            log.error(e.getMessage());
        }
    }
    public void closeConsumer() {
        this.kafkaConsumer.close();
    }

    public static void main(String[] args) {
        String topicName = "order-serde-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrderDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "order-serde-group");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        OrderSerdeConsumerV2 orderSerdeConsumerV2 = new OrderSerdeConsumerV2(props, List.of(topicName));
        orderSerdeConsumerV2.initConsumer();
        String commitMode = "async";

        orderSerdeConsumerV2.pollConsumes(100, commitMode);
        orderSerdeConsumerV2.closeConsumer();

    }


}