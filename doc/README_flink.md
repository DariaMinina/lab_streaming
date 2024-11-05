# Запуск программы .py вместе с запуском кластера

При установке зависимостей в `requirements.txt` в виртуальном окружении появляется директория `venv/lib/python3.8/site-packages/pyflink/bin`. В нее необходимо перейти.

```
cd venv/lib/python3.8/site-packages/pyflink/bin
```

## В директории `venv/lib/python3.8/site-packages/pyflink/bin` запустите кластер

```
./start-cluster.sh 
```

## После этого запустите сам flink с python-файлом, который у вас имеется

```
./flink run --python abs/path/to/flink_consumer.py
```

Обратите внимание: я запускаю flink из рабочей директории `venv/lib/python3.8/site-packages/pyflink/bin`, при запуске из корневой директории проекта возникали проблемы с поиском `jar`-файлов. 

Теперь можно открыть http://localhost:8081 и наслаждаться рабочим job-ом.


## Остановите кластер после работы

```
./stop-cluster.sh 
```