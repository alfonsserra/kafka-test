package com.systelab.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Scanner;

public class ConsumerWithThreadExample extends ConsumerExample {

    private static final Logger logger = LogManager.getLogger(ConsumerWithThreadExample.class);

    private class ConsumerThread extends Thread {
        private KafkaConsumer<String, String> consumer;

        public ConsumerThread() {
        }

        public void run() {
            consumer = getKafkaConsummer(ConsumerExample.TOPIC, ConsumerExample.GROUP);
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records)
                        logger.info("Message partition->" + record.partition() + " stored at offset->" + record.offset() + ": " + record.value());
                }
            } catch (WakeupException ex) {
                logger.error("Wakeup requested");
            } finally {
                consumer.close();
            }
        }

        public void wakeup() {
            consumer.wakeup();

        }

        public KafkaConsumer<String, String> getKafkaConsumer() {
            return this.consumer;
        }
    }

    public void consume() {
        try {
            Scanner in = new Scanner(System.in);
            ConsumerThread consumerRunnable = new ConsumerThread();
            consumerRunnable.start();
            String line = "";
            while (!line.equals("exit")) {
                line = in.next();
            }
            logger.info("Stopping consumer ...");
            consumerRunnable.wakeup();
            consumerRunnable.join();
        } catch (Exception ex) {
            logger.error("Exception caught", ex);
        }
    }

    public static void main(String[] argv) throws Exception {
        new ConsumerWithThreadExample().consume();
    }
}
