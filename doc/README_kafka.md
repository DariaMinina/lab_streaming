## Гайд по развертыванию Kafka

### Скачать актуальную версию Kafka с сайта

https://kafka.apache.org/downloads

### Распаковать .tgz файл в нужной директории (у меня - директория проекта)

```
tar -xzf kafka-3.8.0-src.tgz
```

### Запустить Zookeeper

```
cd kafka-3.8.0-src
```

```
./gradlew jar -PscalaVersion=2.13.14
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### Запустить Kafka server (эта команда запустить брокер)

```
bin/kafka-server-start.sh config/server.properties
```

### Создание топика

```
bin/kafka-topics.sh --create --topic cities --bootstrap-server localhost:9092
```

### Вывод списка всех топиков

```
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Producing в топик

```
bin/kafka-console-producer.sh --topic cities --bootstrap-server localhost:9092
```

### Consuming в топике

```
bin/kafka-console-consumer.sh --topic cities --bootstrap-server localhost:9092
```

## Consuming в топике с начала

```
bin/kafka-console-consumer.sh --topic cities --from-beginning --bootstrap-server localhost:9092
```

## Создание топика с 1 партицией и 1 репликацией

```
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic raw_earthquakes_data_topic
```