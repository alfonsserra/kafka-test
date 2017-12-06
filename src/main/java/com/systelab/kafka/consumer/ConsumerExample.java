package com.systelab.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerExample {
    private static final Logger logger = LogManager.getLogger(ConsumerExample.class);

    public static String TOPIC = "modulab";
    public static String GROUP = "21";

    protected Properties getKafkaProperties(String groupId) {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");
        return configProperties;
    }

    public void consume() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(getKafkaProperties(ConsumerExample.GROUP));
        consumer.subscribe(Arrays.asList(ConsumerExample.TOPIC));
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
