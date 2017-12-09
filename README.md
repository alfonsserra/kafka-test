# Testing Apache Kafka

Simple producers and consumers for Apache Kafka

## Getting Started

1.  Visit the [Apache Kafka download page][kafka] to install the most recent version (1.0.0 as of this writing).
2.	Extract the binaries into a software/kafka folder. For the current version, it's software/kafka_2.12-1.0.0.
3.	Change your current directory to point to the new folder.
4.	Run Apache ZooKeeper and Kafka:

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```
5.  Create a test topic to use for testing:

```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic modulab
```

[kafka]: http://kafka.apache.org/downloads.html

## Working with the repo

```bash
git clone https://github.com/alfonsserra/kafka-test.git
cd kafka-test
mvn compile assembly:single
```
