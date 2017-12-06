package com.systelab.kafka.producer;

import com.systelab.kafka.model.Patient;
import org.apache.kafka.clients.producer.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Properties;

public class ProducerWithPartitionsExample extends ProducerExample {

    private static final Logger logger = LogManager.getLogger(ProducerWithPartitionsExample.class);

    protected Properties getKafkaProperties() {
        Properties configProperties = super.getKafkaProperties();
        configProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CountryPartitioner.class.getCanonicalName());
        configProperties.put("partitions.0", "USA");
        configProperties.put("partitions.1", "India");
        return configProperties;
    }

    protected void send(Producer producer, Patient patient) {
        ProducerRecord<String, String> rec = new ProducerRecord<String, String>(ProducerExample.TOPIC, patient.getCountry(), patient.toJSON());
        producer.send(rec, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                logger.info("Message sent to topic->" + metadata.topic() + ", partition->" + metadata.partition() + " stored at offset->" + metadata.offset());
            }
        });
    }
    public static void main(String[] argv) throws Exception {
        new ProducerWithPartitionsExample().produce();
    }
}

