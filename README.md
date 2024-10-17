# lab1

### Создайте виртуальное окружение (Python >= 3.8.1)

```
python -m venv venv
```

### Активируйте виртуальное окружение

```
source venv/bin/activate
```

### Загрузите нужные библиотеки

```
pip install -r requirements.txt
```

## Предметная область

Чтение уведомлений почти в режиме реального времени о новых и обновленных событиях землетрясений с помощью протокола WebSocket. Любой клиент WebSocket может подключиться к сервису `wss://www.seismicportal.eu/standing_order/websocket`, чтобы получить уведомление. Пример кода на `Python` предоставлен в файле `test.py`.

`https://www.seismicportal.eu/webservices.html` - основные сервисы

Полученное сообщение имеет вид:

```
{
    "action":"create",
    "data":{
        "type":"Feature",
        "geometry":{
            "type":"Point",
            "coordinates":[
                -82.876,
                9.02,
                -20.0
            ]
        },
        "id":"20241004_0000179",
        "properties":{
            "source_id":"1714196",
            "source_catalog":"EMSC-RTS",
            "lastupdate":"2024-10-04T16:56:50.496103Z",
            "time":"2024-10-04T16:28:30.0Z",
            "flynn_region":"PANAMA-COSTA RICA BORDER REGION",
            "lat":9.02,
            "lon":-82.876,
            "depth":20.0,
            "evtype":"ke",
            "auth":"UCR",
            "mag":3.1,
            "magtype":"m",
            "unid":"20241004_0000179"
        }
    }
}
```

Группировать сообщения во временное окно (сравнивать координаты)
среднее координата по всем событиям в окне, найти эпицентр (взвешенная сумма)
хранение с журналированием

чувствительность к выбросам 