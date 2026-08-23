package com.example.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    private Logger log = LoggerFactory.getLogger(CustomPartitioner.class);
    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();
    private String specialKeyName;


    @Override
    public void configure(Map<String, ?> configs) {
        specialKeyName = configs.get("custom.specialKey").toString();
    }

    /**
     * @param topic Topic 명
     * @param cluster 브로커에 있는 Topic의 정보들을 캐싱해서 가지고 있는 정보
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
        int numPartitions = partitionInfoList.size();
        int numSpecialPartitions = (int)(numPartitions * 0.5);
        int partitionIndex = 0;

        if (keyBytes == null) {
//            return stickyPartitionCache.partition(topic, cluster);
            throw new IllegalArgumentException("key should not be null");
        }

        if(((String)key).equals(specialKeyName)){
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % numSpecialPartitions;
        }else{
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % (numPartitions - numSpecialPartitions) + numSpecialPartitions;
        }
        log.info("key:{} is sent to partition:{}", key.toString(), partitionIndex);

        return partitionIndex;
    }

    @Override
    public void close() {

    }
}
