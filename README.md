### Краткое описание
Реализация функционала ETL скрипта 

### В проекте использованы

* [Python](https://www.python.org/)
* [PySpark](https://spark.apache.org/docs/latest/api/python/)


### Установка и запуск

Описание по быстрому локальному запуску проекта 

1. Склонируйте репозиторий
   ```sh
   $ git clone https://github.com/MaxFallishe/ETL_script_task__megaphoneOneDayOfferPython_assembly
   ```
2. Установите Docker

3. Создайте Docker образ
   ```sh
   sudo docker build -t pyspark --build-arg PYTHON_VERSION=3.7.10 --build-arg IMAGE=buster .
   ```
4. Запустите Docker образ
   ```sh
   sudo docker run -it pyspark
   ```
5. Запустите файл main.py
   ```sh
   python main.py
   ```

## Контакты

[@NikitaBorsov](https://t.me/NikitaBorsov) - Telegram


