package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerASync {

    public static final Logger log = LoggerFactory.getLogger(SimpleProducerASync.class.getName());

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
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "Hello World2");

        //KafkaProducer message send
//        kafkaProducer.send(producerRecord, new Callback() {
//            @Override
//            public void onCompletion(RecordMetadata metadata, Exception exception) {
//                if(exception == null){
//                    log.info("\n ####### record metadata recieved #######\n" +
//                            "partitions:" + metadata.partition() + "\n" +
//                            "offse:" + metadata.offset() + "\n" +
//                            "timestamp:" + metadata.timestamp());
//                }else{
//                    log.error("exception error from broker" + exception.getMessage());
//                }
//            }
//        });
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if(exception == null) {
                log.info("\n ####### record metadata recieved #######\n" +
                        "partitions:" + metadata.partition() + "\n" +
                        "offse:" + metadata.offset() + "\n" +
                        "timestamp:" + metadata.timestamp());
            }else{
                log.error("exception error from broker" + exception.getMessage());
            }
        });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }
}
