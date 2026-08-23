package com.example.kafka;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class PizzaProducerCustomPartitioner {

    public static final Logger log = LoggerFactory.getLogger(PizzaProducerCustomPartitioner.class.getName());

    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
                                        String topicName, int iterCount,
                                        int interIntervalMillis, int intervalMillis,
                                        int intervalCount, boolean sync) {

        PizzaMessage pizzaMessage = new PizzaMessage();
        int iterSeq = 0;
        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        long startTime = System.currentTimeMillis();

        while( iterSeq++ != iterCount ) {
            HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,
                    pMessage.get("key"), pMessage.get("message"));
            sendMessage(kafkaProducer, producerRecord, pMessage, sync);

            if((intervalCount > 0) && (iterSeq % intervalCount == 0)) {
                try {
                    log.info("####### IntervalCount:" + intervalCount +
                            " intervalMillis:" + intervalMillis + " #########");
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
            }

            if(interIntervalMillis > 0) {
                try {
                    log.info("interIntervalMillis:" + interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
            }

        }
        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;

        log.info("{} millisecond elapsed for {} iterations", timeElapsed, iterCount);

    }

    public static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                   ProducerRecord<String, String> producerRecord,
                                   HashMap<String, String> pMessage, boolean sync) {
        if(!sync) {
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    log.info("async message:" + pMessage.get("key") + " partition:" + metadata.partition() +
                            " offset:" + metadata.offset());
                } else {
                    log.error("exception error from broker " + exception.getMessage());
                }
            });
        } else {
            try {
                RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
                log.info("sync message:" + pMessage.get("key") + " partition:" + metadata.partition() +
                        " offset:" + metadata.offset());
            } catch (ExecutionException e) {
                log.error(e.getMessage());
            } catch (InterruptedException e) {
                log.error(e.getMessage());
            }
        }

    }


    public static void main(String[] args) {
        String topicName = "pizza-topic-partitioner";

        //KafkaProducer configuration setting
        //null, "hello world"

        Properties props = new Properties();

        //bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.setProperty("partitioner.class", "CustomPartitioner");
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.example.kafka.CustomPartitioner");
        // id 값에 따라 커스텀한 파티셔널에서 특정 파티션으로 보내기 위한 코드
        // 상속받은 configure에 인자로 들어오기 때문에 하드코딩하지 않고 인자를 보낼 수 있다.
        props.setProperty("custom.specialKey", "P001");


        //KafkaProducer Object Create
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        sendPizzaMessage(kafkaProducer, topicName, -1, 100, 0, 0, true);

        kafkaProducer.close();
    }
}
