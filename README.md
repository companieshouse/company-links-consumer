company-links-consumer
=========================

company-links-consumer is responsible for determining if company links need to be updated

## Running kafka locally
From root folder of this project run ```docker-compose up -d```

Once containers up, run ```docker-compose exec kafka bash``` to enter kafka bash to create topics

### Create kafka topics locally
kafka-topics.sh --create   --zookeeper zookeeper:2181   --replication-factor 1 --partitions 1   --topic main-topic

kafka-topics.sh --create   --zookeeper zookeeper:2181   --replication-factor 1 --partitions 1   --topic retry-topic

kafka-topics.sh --create   --zookeeper zookeeper:2181   --replication-factor 1 --partitions 1   --topic error-topic

### Create kafka topics locally
kafka-topics.sh --list --zookeeper zookeeper:2181

### Produce kafka test messages locally
kafka-console-producer.sh --topic delta-topic --broker-list localhost:9092