package com.systelab.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;


public class ConsumerWithThreadExample {

    private static final Logger logger = LogManager.getLogger(ConsumerWithThreadExample.class);

    public static String TOPIC = "modulab";
    public static String GROUP = "21";

    public static void main(String[] argv) throws Exception {

        Scanner in = new Scanner(System.in);
        ConsumerThread consumerRunnable = new ConsumerThread(ConsumerWithThreadExample.TOPIC, ConsumerWithThreadExample.GROUP);
        consumerRunnable.start();
        String line = "";
        while (!line.equals("exit")) {
            line = in.next();
        }
        consumerRunnable.getKafkaConsumer().wakeup();
        logger.info("Stopping consumer ...");
        consumerRunnable.join();
    }

    private static class ConsumerThread extends Thread {
        private String topicName;
        private String groupId;
        private KafkaConsumer<String, String> consumer;

        public ConsumerThread(String topicName, String groupId) {
            this.topicName = topicName;
            this.groupId = groupId;
        }

        protected Properties getKafkaProperties(String groupId) {
            Properties configProperties = new Properties();
            configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");
            return configProperties;
        }
        public void run() {
            consumer = new KafkaConsumer<String, String>(getKafkaProperties(groupId));
            consumer.subscribe(Arrays.asList(topicName));
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records)
                        logger.info("Message partition->" + record.partition() + " stored at offset->" + record.offset() + ": " + record.value());
                }
            } catch (WakeupException ex) {
                logger.error("Exception caught", ex);
            } finally {
                consumer.close();
            }
        }

        public KafkaConsumer<String, String> getKafkaConsumer() {
            return this.consumer;
        }
    }
}

