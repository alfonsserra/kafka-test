package com.systelab.kafka.producer;

import com.systelab.kafka.model.Patient;
import org.apache.kafka.clients.producer.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Properties;

public class ProducerExample {

    private static final Logger logger = LogManager.getLogger(ProducerExample.class);

    public static String TOPIC = "modulab";

    protected Properties getKafkaProperties() {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        configProperties.put(ProducerConfig.LINGER_MS_CONFIG,"2000");
        configProperties.put(ProducerConfig.ACKS_CONFIG,"1"); //Leader acknowledgement
        return configProperties;
    }

    protected void send(Producer producer, Patient patient) {
        ProducerRecord<String, String> rec = new ProducerRecord<String, String>(ProducerExample.TOPIC, null, patient.toJSON());
        producer.send(rec, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                logger.info("Message sent to topic->" + metadata.topic() + ", partition->" + metadata.partition() + " stored at offset->" + metadata.offset());
            }
        });
    }

    protected void send(Producer producer) {
        for (int i = 0; i < 1000; i++) {
            Patient patient = new Patient(new Long(i), "Peter", "Caramaro", i % 2 == 0 ? "USA" : "India");
            send(producer, patient);
        }
    }

    public void produce() {
        Producer producer = new KafkaProducer(getKafkaProperties());
        send(producer);
        try {
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        producer.close();
    }

    public static void main(String[] argv) throws Exception {
        new ProducerExample().produce();
    }
}
