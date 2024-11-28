## Гайд по развертыванию AKHQ

После развертывания Kafka кластера, можно подключить [akhq](https://akhq.io/docs/installation.html) для удобного отображения kafka топиков. В этом проекте akhq был установлен локально, так как сам kafka кластер тоже развернут локально, тем самым, если разворачивать akhq в docker, например, то необходим доступ на соединение по порту, на котором крутится кластер. 

###

Для установки и развертывания был реализован пункт **Stand Alone** по ссылке [akhq](https://akhq.io/docs/installation.html). 

В файле у кафка кластера (путь `kafka-3.8.0-src/config/server.properties`) с настройками сервера необходимо раскомментить строку
```
listeners=PLAINTEXT://127.0.0.1:9092
```

Также для корректного запуска [akhq](https://akhq.io/docs/installation.html) нужна Java 17:

```
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home
```

После этого экспорта можно запускать .jar-файл, который вы скачали раннее:

```
java -Dmicronaut.config.files=/path/to/application.yml -jar akhq.jar
```