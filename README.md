# Testing Apache Kafka

Simple producers and consumers for Apache Kafka

## Working with the repo

```bash
git clone https://github.com/alfonsserra/kafka-test.git
cd kafka-test
mvn compile assembly:single
```

## Prerequisites

Run zookeeper and Apache ZooKeeper and Kafka:

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```
Create a test topic to use for testing:

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic modulab
```
