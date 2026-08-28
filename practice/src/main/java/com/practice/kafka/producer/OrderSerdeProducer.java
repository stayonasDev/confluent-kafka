package com.practice.kafka.producer;

import com.practice.kafka.model.OrderModel;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class OrderSerdeProducer {
    public static final Logger log = LoggerFactory.getLogger(OrderSerdeProducer.class.getName());

    public static void main(String[] args) {
        String topicName = "order-serde-topic";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSerializer.class.getName());

        KafkaProducer<String, OrderModel> kafkaProducer = new KafkaProducer<>(props);
        String filePath = "D:\\PorjectFile\\KafkaProject-01\\practice\\src\\main\\resources\\pizza_sample.txt";

        //Kafka Producer 객체 생성 -> Producer Records 생성 -> send() 비동기 방식 전송
        sendFileMessage(kafkaProducer, topicName, filePath);

        kafkaProducer.close();
    }

    private static void sendFileMessage(KafkaProducer<String, OrderModel> kafkaProducer, String topicName, String filePath) {
        String line = "";
        final String delimiter = ",";

        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            while ((line = bufferedReader.readLine()) != null) {
                String[] tokens = line.split(delimiter);
                String key = tokens[0];

                OrderModel orderModel = new OrderModel(tokens[1], tokens[2], tokens[3],
                        tokens[4], tokens[5], tokens[6], LocalDateTime.parse(tokens[7], formatter));

                sendMessage(kafkaProducer, topicName, key, orderModel);
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    private static void sendMessage(KafkaProducer<String, OrderModel> kafkaProducer, String topicName, String key, OrderModel value) {
        ProducerRecord<String, OrderModel> producerRecord = new ProducerRecord<>(topicName, key, value);
        log.info("key:{}, value:{}", key, value);

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
    }
}
