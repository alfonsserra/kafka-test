package com.systelab.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class ConsumerExample {
    private static final Logger logger = LogManager.getLogger(ConsumerExample.class);

    public static String TOPIC = "modulab";
    public static String GROUP = "group:high";

    protected Properties getKafkaProperties(String groupId) {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); //default value. Do not commit the messages manually
        configProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000); //default value
        configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //default value. Other: earliest and none

        if (groupId!=null && !groupId.equals("")) {
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "id:1");
        }
        return configProperties;
    }

    public KafkaConsumer<String, String> getKafkaConsummer(String topic, String group) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(getKafkaProperties(group));
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                logger.info(Arrays.toString(partitions.toArray()) + " topic-partitions are revoked from this consumer\n");
            }

            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                logger.info(Arrays.toString(partitions.toArray()) + " topic-partitions are assigned to this consumer\n");
            }
        });
        return consumer;
    }

    public void consume() {
        KafkaConsumer<String, String> consumer = getKafkaConsummer(ConsumerExample.TOPIC, ConsumerExample.GROUP);
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                    logger.info("Message partition->" + record.partition() + " stored at offset->" + record.offset() + ": " + record.value());
            }
        } catch (Exception ex) {
            logger.error("Exception caught", ex);
        } finally {
            consumer.close();
        }
    }

    public static void main(String[] argv) throws Exception {
        new ConsumerExample().consume();
    }
}
