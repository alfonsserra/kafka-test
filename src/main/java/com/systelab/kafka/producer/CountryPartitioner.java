package com.systelab.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CountryPartitioner implements Partitioner {
    private static Map<String, Integer> countryToPartitionMap;

    // This method will gets called at the start, you should use it to do one time startup activity
    public void configure(Map<String, ?> configs) {
        countryToPartitionMap = new HashMap<String, Integer>();
        for (Map.Entry<String, ?> entry : configs.entrySet()) {
            if (entry.getKey().startsWith("partitions.")) {
                String keyName = entry.getKey();
                String value = (String) entry.getValue();
                int paritionId = Integer.parseInt(keyName.substring(11));
                countryToPartitionMap.put(value, paritionId);
            }
        }
    }

    //This method will get called once for each message
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
                         Cluster cluster) {
        List partitions = cluster.availablePartitionsForTopic(topic);
        if (countryToPartitionMap.containsKey(key)) {
            //If the country is mapped to particular partition return it
            return countryToPartitionMap.get(key);
        } else {
            //If no country is mapped to particular partition distribute between remaining partitions
            int noOfPartitions = cluster.topics().size();
            return value.hashCode() % noOfPartitions + countryToPartitionMap.size();
        }
    }

    // This method will get called at the end and gives your partitioner class chance to cleanup
    public void close() {
    }
}
