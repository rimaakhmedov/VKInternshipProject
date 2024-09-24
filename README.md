# VKInternshipProject
Тестовое задание на стажировку на инженера данных в ВК

[Образ Airflow на котором собран проект](https://github.com/halltape/HalltapeETL)


`docker-compose up -d` для сборки проекта

### Содержимое проекта
generate.py - генерирует csv файлы для дальнейшего преобразования

Пример запуска generate.py:

`python generate.py <dir to put files> <%Y-%m-%d date> <days_count> <unique emails count> <events count>`

input - содержит уже сгенерированные csv файлы

spark_count_script.py - вычисляет сумму всех действий пользователя за прошлые 7 дней

Пример запуска spark_count_script.py:

`python spark_count_script.py <YYYY-mm-dd> <input/directory> <output/directory>`

dags/spark_count_dag.py - dag где по расписанию каждый день в 7:00 запускается вычисление недельного агрегата

Итоговый файл с агрегатом будет находиться в папке output
