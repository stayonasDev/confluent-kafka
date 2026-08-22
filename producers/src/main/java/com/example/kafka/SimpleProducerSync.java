package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleProducerSync {

    public static final Logger log = LoggerFactory.getLogger(SimpleProducerSync.class.getName());

    public static void main(String[] args) {
        String topicName = "simple-topic";
        //KafkaProducer configuration setting
        //null, "hello world"

        Properties props = new Properties();

        //bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //KafkaProducer Object Create
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        //ProducerRecord Object Create
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "Hello World");

        //KafkaProducer message send
        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            log.info("\n ####### record metadata recieved #######\n" +
                    "partitions:" + recordMetadata.partition() + "\n" +
                    "offse:" + recordMetadata.offset() + "\n" +
                    "timestamp:" + recordMetadata.timestamp());
        }catch(InterruptedException e) {
            e.printStackTrace();
        }catch(ExecutionException e){
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }
}
