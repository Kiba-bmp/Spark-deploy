# Spark на YARN кластера Team B

Домашка номер четыре: запустить Apache Spark под управлением нашего YARN, прочитать данные из нашего HDFS, преобразовать их, сохранить как партиционированную таблицу и проверить, что её видит обычный клиент Hive.

Как и третью, эту домашку мы оформили как описание: ниже все шаги, команды, конфиги и сам код по порядку. Всё работает поверх того, что уже развёрнуто: HDFS из первой домашки, YARN из второй и Hive из третьей. Team A не трогаем.

## Что и как закрывает задание

Чтобы было проще проверять, сразу по пунктам задания:

1. Сессия Spark запускается через spark-submit с master yarn, то есть приложение получает контейнеры у нашего ResourceManager. Скрипт печатает master и id приложения в YARN, а само приложение видно в yarn application -list и в YARN UI.
2. С HDFS Spark работает через наш core-site.xml (fs.defaultFS = hdfs://team-28-nn:9100). Входные данные читаем по полному адресу hdfs://team-28-nn:9100/..., чтобы было явно видно, что это наш HDFS, а не HDFS Team A на 9000.
3. CSV заранее кладём в HDFS командой hdfs dfs -put, Spark читает его оттуда в той же сессии.
4. Трансформаций несколько: приведение типов (строки в bigint, int, decimal, timestamp), чистка значений (trim и lower для типа события), фильтрация битых строк, новые колонки (дата и час из времени события) и агрегация по дате и типу события (count, count distinct, sum, avg, min, max).
5. Результат пишется с партиционированием по event_date, в HDFS получается отдельный каталог на каждую дату.
6. Результат сохраняется как таблица team28b.events_daily_stats в метасторе Hive из третьей домашки.
7. Таблицу читаем через beeline, подключившись к нашему HiveServer2, и сверяем числа с тем, что посчитал Spark.

## Раскладка по узлам

Spark не нужно запускать как отдельный сервис: это клиентская библиотека, кластером управляет YARN. Поэтому Spark ставим только на team-28-01 (10.28.0.13), туда же, где Hive из третьей домашки. Оттуда запускаем spark-submit.

Режим client: драйвер работает на team-28-01, а ApplicationMaster и executor'ы YARN запускает в контейнерах на наших NodeManager'ах на всех трёх узлах. На team-28-nn и team-28-00 ничего ставить не нужно, jar-файлы Spark YARN сам раздаст из HDFS.

Подключение как раньше: с ноутбука на edge team-28-en под team28b, оттуда на team-28-01 по ключу team28b_internal.

## Версии

Spark 3.5.5, сборка spark-3.5.5-bin-hadoop3. Spark 4 нам не подходит, ему нужна Java 17, а у нас Java 11 (Temurin) с первой домашки. Ветка 3.5 работает с Java 11, и в эту сборку уже входит поддержка Hive.

Внутри сборки клиент Hadoop 3.3.4, а кластер у нас 3.4.3. Это нормально, клиент третьей ветки совместим с HDFS и YARN 3.4.

Код пишем на PySpark. Нужен python3, на наших Ubuntu он есть из коробки, проверяем на всех трёх узлах:

```bash
python3 --version
```

Нужен Python 3.8 или новее.

## Где всё лежит

Spark в ~/apps/spark, конфиги в ~/apps/spark/conf. Код задачи в ~/spark/jobs, тестовые данные в ~/spark/data, логи запусков в ~/spark/logs.

В HDFS: входные данные в /user/team28b/spark/input/events, jar-файлы Spark для YARN в /user/team28b/spark/jars, данные итоговой таблицы в /user/team28b/spark/tables/events_daily_stats.

## Порты

Сервисов Spark не поднимает, поэтому новых постоянных портов нет. Единственное - Spark UI драйвера, пока работает задача. По умолчанию это 4040, его может занять Team A, поэтому у нас 4140, как и раньше со сдвигом +100.

Остальные порты уже наши из прошлых домашек: HDFS 9100, ResourceManager UI 9088, Metastore 9183, HiveServer2 10100. Ещё один добавляется в этой домашке: клиентский порт ResourceManager 9033, про него следующий раздел.

## Адрес ResourceManager для клиентов

Во второй домашке мы перенесли на свои порты планировщик, трекер ресурсов, admin и Web UI, но не клиентский адрес yarn.resourcemanager.address. Пока задачи не запускались, это было незаметно. А spark-submit как раз клиент YARN: через этот адрес он отдаёт приложение ResourceManager'у.

Без этого свойства ResourceManager слушает порт по умолчанию 8032. Это плохо по двум причинам. Во-первых, 8032 - стандартный порт, его займёт YARN Team A. Во-вторых, адрес собирается из имени team-28-nn, а оно на самой этой машине резолвится ещё и в 127.0.1.1, так что порт может оказаться доступен только изнутри team-28-nn, и с team-28-01 Spark до него не достучится. Это та же история с 127.0.1.1, что и во второй домашке.

Поэтому задаём адрес явно, через IP team-28-nn и свободный порт из нашего диапазона. Сначала проверяем на team-28-nn, что 9033 никто не занял:

```bash
ss -ltn | grep -E ':9033\b' || echo СВОБОДЕН
```

Дописываем свойство в yarn-site.xml на всех трёх узлах, чтобы конфиги оставались одинаковыми. Команда добавляет блок перед закрывающим тегом configuration и ничего не делает, если свойство уже есть:

```bash
grep -q 'yarn.resourcemanager.address' ~/apps/hadoop/etc/hadoop/yarn-site.xml || sed -i '$i\    <property>\n        <name>yarn.resourcemanager.address</name>\n        <value>10.28.0.11:9033</value>\n    </property>' ~/apps/hadoop/etc/hadoop/yarn-site.xml
grep -A1 'yarn.resourcemanager.address' ~/apps/hadoop/etc/hadoop/yarn-site.xml
```

Перезапускаем только ResourceManager, на team-28-nn. NodeManager'ы трогать не нужно, они ходят к нему через трекер на 9031, который не менялся, и сами переподключатся:

```bash
yarn --daemon stop resourcemanager
yarn --daemon start resourcemanager
ss -ltn | grep -E ':9033\b'
```

Порт 9033 должен слушаться на 10.28.0.11.

## Что должно работать перед стартом

Всё из прошлых домашек должно быть запущено: HDFS, YARN, а из Hive - PostgreSQL и Metastore. HiveServer2 нужен только для последнего шага, проверки через beeline.

Проверяем на team-28-01:

```bash
hdfs dfsadmin -report | grep -E 'Live datanodes'
yarn node -list
ss -ltn | grep -E ':(9183|10100)\b'
```

hdfs должен показать 3 живых DataNode, yarn - 3 ноды в состоянии RUNNING, и должны слушаться 9183 (Metastore) и 10100 (HiveServer2). Если yarn node -list отвечает, значит клиент YARN на этой машине видит наш ResourceManager, и spark-submit его тоже найдёт: Spark берёт адрес из того же yarn-site.xml.

## Установка Spark

```bash
cd ~/apps
curl -L -o spark.tgz https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
tar -xzf spark.tgz && mv spark-3.5.5-bin-hadoop3 spark && rm spark.tgz
mkdir -p ~/spark/jobs ~/spark/data ~/spark/logs
```

Переменные в ~/.bashrc:

```bash
echo 'export SPARK_HOME=$HOME/apps/spark' >> ~/.bashrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
spark-submit --version
```

## spark-env.sh

Здесь говорим Spark, где Java и конфиги нашего Hadoop. Именно через HADOOP_CONF_DIR и YARN_CONF_DIR Spark узнаёт, где наш NameNode и наш ResourceManager:

```bash
cat > ~/apps/spark/conf/spark-env.sh << 'EOF'
export JAVA_HOME=$HOME/apps/java
export HADOOP_CONF_DIR=$HOME/apps/hadoop/etc/hadoop
export YARN_CONF_DIR=$HOME/apps/hadoop/etc/hadoop
export PYSPARK_PYTHON=python3
EOF
```

## Jar-файлы Spark в HDFS

При каждом запуске на YARN Spark по умолчанию пакует все свои jar-файлы (а это сотни мегабайт) и заливает их в HDFS заново. Чтобы так не было, один раз кладём их в HDFS сами и потом указываем на них в конфиге:

```bash
hdfs dfs -mkdir -p /user/team28b/spark/jars
hdfs dfs -put -f ~/apps/spark/jars/*.jar /user/team28b/spark/jars/
hdfs dfs -count /user/team28b/spark/jars
```

## spark-defaults.conf

```bash
cat > ~/apps/spark/conf/spark-defaults.conf << 'EOF'
spark.master                     yarn
spark.submit.deployMode          client
spark.yarn.jars                  hdfs://team-28-nn:9100/user/team28b/spark/jars/*.jar

spark.driver.host                10.28.0.13
spark.driver.bindAddress         0.0.0.0
spark.ui.port                    4140

spark.driver.memory              512m
spark.yarn.am.memory             512m
spark.executor.memory            512m
spark.executor.cores             1
spark.executor.instances         2

spark.sql.catalogImplementation  hive
spark.sql.warehouse.dir          /user/hive/warehouse
EOF
```

Что тут зачем.

spark.master yarn - это и есть запуск под управлением YARN. Режим client выбрали, чтобы вывод задачи печатался прямо в терминал.

spark.driver.host - снова та же история, что во второй и третьей домашке: имя team-28-01 на самой машине резолвится ещё и в 127.0.1.1. В режиме client executor'ы с других узлов подключаются обратно к драйверу, и если драйвер объявит себя по loopback-адресу, они до него не дойдут. Поэтому указываем IP явно, а слушать драйвер будет на всех интерфейсах.

Память ставим маленькую. На каждом узле около 2.5 ГБ, и там уже работают DataNode, NodeManager, а на team-28-01 ещё PostgreSQL и Hive. При этом NodeManager'ы думают, что у них по 8 ГБ (это осталось по умолчанию со второй домашки), так что YARN сам нас не ограничит, приходится следить самим. YARN всё равно добавит к каждому контейнеру 384 МБ накладных и округлит до 1 ГБ, итого на задачу 3 контейнера по 1 ГБ: ApplicationMaster и два executor'а.

spark.sql.catalogImplementation hive - таблицы Spark хранит в метасторе Hive, а не в своём временном каталоге в памяти.

## hive-site.xml для Spark

Чтобы Spark работал с тем же метастором, что и Hive из третьей домашки, кладём ему маленький hive-site.xml. Целиком копировать конфиг Hive не нужно: Spark ходит в метастор по thrift, в PostgreSQL сам он не подключается, и пароль от базы ему знать незачем.

```bash
cat > ~/apps/spark/conf/hive-site.xml << 'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://10.28.0.13:9183</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
  </property>
</configuration>
EOF
```

## Быстрая проверка, что сессия поднимается на YARN

Перед основной задачей можно убедиться, что всё связано, через интерактивный pyspark:

```bash
pyspark
```

Внутри:

```python
spark.sparkContext.master
spark.sparkContext.applicationId
spark.sql("SHOW DATABASES").show()
```

master должен быть yarn, applicationId вида application_..., а в списке баз должна быть team28b из третьей домашки. Если она видна, значит Spark ходит в наш метастор. Выходим через exit().

## Тестовые данные в HDFS

Генерируем CSV на awk, 5000 событий за 5 дней. Данные специально немного грязные, чтобы трансформациям было что делать: тип события иногда записан большими буквами или с пробелом в начале, а у каждой 250-й строки нет времени события (такие строки фильтр должен выкинуть, их ровно 20).

```bash
awk -v n=5000 'BEGIN {
  srand(28)
  split("view,click,purchase,refund,VIEW, Click", t, ",")
  print "event_id,user_id,event_type,amount,event_time"
  for (i = 1; i <= n; i++) {
    ts = sprintf("2026-10-%02d %02d:%02d:%02d", 1+int(rand()*5), int(rand()*24), int(rand()*60), int(rand()*60))
    if (i % 250 == 0) ts = ""
    printf "%d,%d,%s,%.2f,%s\n", i, 1+int(rand()*200), t[1+int(rand()*6)], rand()*1000, ts
  }
}' > ~/spark/data/events.csv

head -3 ~/spark/data/events.csv
wc -l ~/spark/data/events.csv
```

wc должен показать 5001 строку: 5000 событий и заголовок.

Кладём в HDFS:

```bash
hdfs dfs -mkdir -p /user/team28b/spark/input/events
hdfs dfs -put -f ~/spark/data/events.csv /user/team28b/spark/input/events/
hdfs dfs -ls /user/team28b/spark/input/events
```

## Код задачи

Сохраняем этот код в файл ~/spark/jobs/events_stats.py:

```python
from pyspark.sql import SparkSession, functions as F

INPUT = "hdfs://team-28-nn:9100/user/team28b/spark/input/events"
TABLE = "team28b.events_daily_stats"
TABLE_PATH = "hdfs://team-28-nn:9100/user/team28b/spark/tables/events_daily_stats"

spark = (
    SparkSession.builder
    .appName("team28b-events-stats")
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()
)
print("master:", spark.sparkContext.master)
print("application id:", spark.sparkContext.applicationId)

# читаем CSV из HDFS, все колонки пока строки
raw = spark.read.option("header", "true").csv(INPUT)
print("прочитано строк:", raw.count())
raw.printSchema()

# типы, чистка, фильтр битых строк, новые колонки
events = (
    raw
    .withColumn("event_id", F.col("event_id").cast("bigint"))
    .withColumn("user_id", F.col("user_id").cast("int"))
    .withColumn("amount", F.col("amount").cast("decimal(10,2)"))
    .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
    .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
    .filter(F.col("event_time").isNotNull())
    .withColumn("event_date", F.date_format("event_time", "yyyy-MM-dd"))
    .withColumn("event_hour", F.hour("event_time"))
)
print("после очистки:", events.count())

# агрегация по дате и типу события
stats = (
    events
    .groupBy("event_date", "event_type")
    .agg(
        F.count("*").alias("events_cnt"),
        F.countDistinct("user_id").alias("users_cnt"),
        F.sum("amount").alias("amount_sum"),
        F.avg("amount").alias("amount_avg"),
        F.min("event_hour").alias("first_hour"),
        F.max("event_hour").alias("last_hour"),
    )
    .select(
        "event_type",
        "events_cnt",
        "users_cnt",
        F.col("amount_sum").cast("decimal(20,2)").alias("amount_sum"),
        F.round("amount_avg", 2).cast("decimal(12,2)").alias("amount_avg"),
        "first_hour",
        "last_hour",
        "event_date",
    )
)

# таблица в метасторе Hive, партиции по дате
spark.sql("CREATE DATABASE IF NOT EXISTS team28b")
spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {TABLE} (
        event_type STRING,
        events_cnt BIGINT,
        users_cnt BIGINT,
        amount_sum DECIMAL(20,2),
        amount_avg DECIMAL(12,2),
        first_hour INT,
        last_hour INT
    )
    PARTITIONED BY (event_date STRING)
    STORED AS ORC
    LOCATION '{TABLE_PATH}'
""")

# запись с динамическими партициями, колонка партиции в select последняя
stats.write.insertInto(TABLE, overwrite=True)

spark.sql(f"SHOW PARTITIONS {TABLE}").show(truncate=False)
spark.table(TABLE).orderBy("event_date", "event_type").show(25, truncate=False)

spark.stop()
```

Пара моментов в коде, которые стоит пояснить.

Таблицу мы создаём сами через CREATE EXTERNAL TABLE, а потом пишем в неё через insertInto. Можно было бы сделать проще, одним saveAsTable, но так хуже по двум причинам. Во-первых, при повторном запуске saveAsTable с внешним путём падает, потому что каталог уже не пустой. Во-вторых, Hive 4 по-своему обращается с managed-таблицами, которые создали сторонние клиенты вроде Spark: может переделать их во внешние и перенести данные в другой каталог. С внешней таблицей и явным LOCATION всё предсказуемо: схему и место хранения задаём мы сами.

Формат ORC выбрали так же, как в третьей домашке. Hive его читает без сюрпризов, в отличие от Parquet, где у Spark и Hive бывают расхождения в том, как хранится decimal.

insertInto сопоставляет колонки по порядку, а не по имени, поэтому в select колонки идут ровно в том порядке, что и в таблице, а event_date последней. С overwrite=True перезаписываются только те партиции, которые есть в новых данных, так что повторный запуск не даёт дублей, а остальные даты не трогает.

## Запуск

```bash
spark-submit --name team28b-events-stats ~/spark/jobs/events_stats.py 2> ~/spark/logs/events_stats.log
```

master и остальные настройки берутся из spark-defaults.conf, поэтому в команде их нет. Служебный лог Spark уходит в файл, а в терминале остаётся только то, что печатает сам скрипт.

Первый запуск занимает пару минут: YARN выделяет контейнеры, executor'ы скачивают jar-файлы из HDFS.

## Проверка в YARN

Пока задача работает и после неё:

```bash
yarn application -list -appStates ALL | grep team28b-events-stats
```

Там видно приложение с типом SPARK, его id совпадает с тем, что напечатал скрипт, а после завершения статус FINISHED и SUCCEEDED.

То же самое видно в YARN UI из второй домашки. Туннель с ноутбука:

```bash
ssh -N -L 9088:team-28-nn:9088 team28b@2.59.83.133
```

В браузере http://localhost:9088, приложение team28b-events-stats будет в списке Applications. Пока задача работает, по ссылке ApplicationMaster открывается Spark UI, там видны executor'ы и на каких узлах они запущены.

## Как понять, что всё получилось

В выводе скрипта:

- master: yarn и application id вида application_..._0001;
- прочитано 5000 строк, в схеме все колонки строковые (это до приведения типов);
- после очистки 4980, то есть 20 строк без времени отфильтровались;
- пять партиций, с event_date=2026-10-01 по event_date=2026-10-05;
- таблица из 20 строк: 5 дат на 4 типа событий. Типов именно четыре, VIEW и " Click" после чистки слились с view и click.

В HDFS:

```bash
hdfs dfs -ls /user/team28b/spark/tables/events_daily_stats
```

Там пять каталогов event_date=..., в каждом ORC-файл. Значит, партиционирование физическое, а не только в метаданных.

## Проверка через Hive

Теперь главное: таблицу, которую записал Spark, читаем обычным клиентом Hive. Подключаемся beeline к нашему HiveServer2 из третьей домашки:

```bash
beeline -u "jdbc:hive2://10.28.0.13:10100/default" -n team28b
```

Внутри:

```sql
SHOW TABLES IN team28b;
DESCRIBE FORMATTED team28b.events_daily_stats;
SHOW PARTITIONS team28b.events_daily_stats;
SELECT * FROM team28b.events_daily_stats WHERE event_date = '2026-10-03';
SELECT SUM(events_cnt) FROM team28b.events_daily_stats;
```

В SHOW TABLES рядом с events из третьей домашки появится events_daily_stats. DESCRIBE FORMATTED покажет тип EXTERNAL_TABLE, формат ORC и путь /user/team28b/spark/tables/events_daily_stats. SHOW PARTITIONS выдаст те же пять дат, что и Spark. Запрос по одной дате вернёт 4 строки, по одной на тип события, с теми же числами, что напечатал Spark. Сумма events_cnt должна быть 4980, ровно столько строк осталось после очистки.

Если Hive видит таблицу, её партиции и те же самые числа, значит Spark и Hive действительно работают с одним метастором и одними данными в HDFS.

## Логи

Служебный лог драйвера - ~/spark/logs/events_stats.log. Логи executor'ов лежат у NodeManager'ов, проще всего смотреть их через YARN UI: приложение, потом Logs у нужного контейнера.

Ошибки считаем как в прошлых домашках:

```bash
grep -icE "error|fatal|exception" ~/spark/logs/events_stats.log
```

Предупреждения о том, что не загрузилась native-hadoop library, и про SLF4J - нормально, на работу не влияют.

## Изоляция от Team A

Spark ставится в /home/team28b, sudo не нужен. Задачи идут в наш YARN, данные читаются и пишутся в наш HDFS на 9100, таблица регистрируется в нашем метасторе на 9183. Spark UI на 4140, клиентский порт ResourceManager на 9033, стандартные 4040 и 8032 не занимаем. Своих долгоживущих процессов Spark не оставляет: после spark.stop() приложение в YARN завершается и контейнеры освобождаются.

## Куда смотреть, если не работает

Если spark-submit долго пишет Connecting to ResourceManager и Retrying connect to server, значит клиент не видит ResourceManager на 10.28.0.11:9033. Проверяем, что свойство yarn.resourcemanager.address есть в yarn-site.xml на team-28-01 и что после правки ResourceManager перезапускали.

Если задача висит на ACCEPTED и не переходит в RUNNING, YARN не может выделить контейнеры. Смотрим yarn node -list: все ли ноды RUNNING. И проверяем, что Team A не заняли сейчас всю память на машинах.

Если контейнер убит с сообщением is running beyond virtual memory limits, это проверка виртуальной памяти в NodeManager, Java 11 часто в неё упирается. Ставим yarn.nodemanager.vmem-check-enabled в false в yarn-site.xml на всех узлах и перезапускаем NodeManager'ы.

Если executor'ы стартуют, но драйвер их не видит и задача висит, скорее всего драйвер объявил себя на 127.0.1.1. Проверяем spark.driver.host в spark-defaults.conf.

Если при работе с таблицами ошибка Invalid method name, значит встроенный в Spark клиент метастора (версии 2.3.9) не договорился с метастором Hive 4. Тогда добавляем в spark-defaults.conf spark.sql.hive.metastore.version 3.1.3 и spark.sql.hive.metastore.jars maven, Spark скачает подходящий клиент при первом запуске.

Если в pyspark не видна база team28b, Spark не нашёл hive-site.xml или метастор не запущен. Проверяем ~/apps/spark/conf/hive-site.xml и порт 9183.

Если beeline не видит таблицу или выдаёт ошибку чтения, проверяем DESCRIBE FORMATTED: путь должен быть в нашем HDFS, а формат ORC.

## Как повторить с нуля

Всё на team-28-01 под team28b. HDFS, YARN, PostgreSQL, Metastore и HiveServer2 из прошлых домашек должны быть запущены. Задаём yarn.resourcemanager.address на всех узлах и перезапускаем ResourceManager. Проверяем python3 и что YARN видит три ноды. Скачиваем Spark, прописываем переменные, создаём spark-env.sh. Кладём jar-файлы Spark в HDFS. Создаём spark-defaults.conf и hive-site.xml для Spark. Проверяем сессию через pyspark. Генерируем CSV и кладём его в HDFS. Сохраняем код в ~/spark/jobs/events_stats.py и запускаем через spark-submit. Проверяем приложение в YARN, партиции в HDFS и таблицу через beeline
