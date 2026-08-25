package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class ConsumerPartitionAssginSeek {

    private static final Logger log = LoggerFactory.getLogger(ConsumerPartitionAssginSeek.class);

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_pizza_assgin_seek_v001");
//        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "6000");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
//        consumer.subscribe(List.of(topicName));
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seek(topicPartition, 10L);

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("main program starts to exit by calling wakeup");
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });


//        pollAutoCommit(consumer);
//        pollCommitSync(consumer);
//        pollCommitAsync(consumer);
        pollNoCommit(consumer);

    }

    private static void pollNoCommit(KafkaConsumer<String, String> consumer) {
        int loopCnt = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                log.info("############# loopCount:{} consumerRecord count:{}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord record : consumerRecords) {
                    log.info("record key:{}, partition:{}, record offset:{}, record value:{}",
                            record.key(), record.partition(), record.offset(), record.value());

                }
            }
        } catch (WakeupException e) {
            log.error("wakeup exception has been called");
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            log.info("finally consumer is closing");
            consumer.close();
        }
    }

    private static void pollCommitAsync(KafkaConsumer<String, String> consumer) {
        int loopCnt = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                log.info("############# loopCount:{} consumerRecord count:{}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord record : consumerRecords) {
                    log.info("record key:{}, partition:{}, record offset:{}, record value:{}",
                            record.key(), record.partition(), record.offset(), record.value());

                }
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception != null) {
                            log.error("offsets {} is not completed, error:{}", offsets, exception.getMessage());
                        }
                    }
                });

            }
        } catch (WakeupException e) {
            log.error("wakeup exception has been called");
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            log.info("##### commit sync before closing");
            consumer.commitSync();
            log.info("finally consumer is closing");
            consumer.close();
        }
    }

    private static void pollCommitSync(KafkaConsumer<String, String> consumer) {
        int loopCnt = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                log.info("############# loopCount:{} consumerRecord count:{}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord record : consumerRecords) {
                    log.info("record key:{}, partition:{}, record offset:{}, record value:{}",
                            record.key(), record.partition(), record.offset(), record.value());

                }
                try {
                    if (consumerRecords.count() > 0) {
                        consumer.commitSync();
                        log.info("commit sync has been called");
                    }
                } catch (CommitFailedException e) {
                    log.error("commit failed: {}", e.getMessage());
                }
            }
        } catch (WakeupException e) {
            log.error("wakeup exception has been called");
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            log.info("finally consumer is closing");
            consumer.close();
        }
    }

    private static void pollAutoCommit(KafkaConsumer<String, String> consumer) {
        int loopCnt = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                log.info("############# loopCount:{} consumerRecord count:{}", loopCnt++, consumerRecords.count());
                for (ConsumerRecord record : consumerRecords) {
                    log.info("record key:{}, partition:{}, record offset:{}, record value:{}",
                            record.key(), record.partition(), record.offset(), record.value());

                }
                try {
                    log.info("main thread is sleeping {} ms during while loop", 10000);
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
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
