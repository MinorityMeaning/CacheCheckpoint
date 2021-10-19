# ResearchDataFrame

В рамках библиотеки *ML360_DM_common* рассмотрена возможность использования функции `checkpoint()` в методе `writeFeatureTable(...)`

#### Таска NBACB-3244:
При тестировании витрины u_calendar_holidays замечено, что в `saveAsStgTable` дважды происходит повторный расчёт DataFrame: `count` и `saveAsTable`. Вероятно это происходит при `autoRepartition`.

Как запустить проект?
--------
- Открыть проект в среде разработки, дождаться прогрузки зависимостей
- Выполнить *Run* sbt-проекта (Предполагается, что среда разработки настроена)

При первом запуске будет выброщено исключение указывающее на отсутствие базы данных. Раскомментируйте метод `writePartToHive`, который вызывается из метода `writeFeatureTable`:
```scala
//Процесс записи в таблицу можно раскомментировать. Если необходимо.

    if (correctPartitions.nonEmpty) {
      writePartToHive(
        sourceRepartitionedDf,
        "pa",
        destinationTableName,
        correctPartitions
      )
    } else {
      sourceRepartitionedDf.write.mode(SaveMode.Overwrite).saveAsPaTable(destinationTableName)
    }

```
После первого запуска в корневом каталоге проекта сохранится таблица в формате `parquet`. Затем, если нет необходимости в методе, следует закомментировать его.

Класс `GeneratorDataFrame` будет генерировать DataFrame из случайных значений со следующими полями:
```scala
val schema = StructType(Array(
    StructField("name_last_name",StringType,true),
    StructField("role",StringType,true),
    StructField("experience",IntegerType,true),
    StructField("born", IntegerType, true),
    StructField("salary", IntegerType, true)
  ))
```  
Переменная `sizeDataFame` объекта `Main` позволяет установить размерность датафрейма.

Вступление
-----
В модуле *U_Calendar_Holidays* библиотеки *ML360_DM* производится расчёт датафрейма праздничных дней. Видим нецелесообразным искать применение кэширования при расчёте праздничных дней (малая размерность датафрейма). Однако `writeFutureTable` может работать с витринами больших размеров, поэтому рассмотрим трассировку датафрейма внутри `writeFeatureTable` : 

![](/img/df-line.jpg)

Из метода `writeFeatureTable` видно, что к датафрейму уже применяется кэширование - `persist()`. А также, по мере продолжения расчётов, в `checkDoubles()` удачно применено кеширование - `cache()`, в чём можно убедиться при локальных тестах. Однако следует немного поговорить об этих методах отдельно.

persist vs cashe
------
Методы кэширования похожи, то есть `cache()` - аналог `persist(StorageLevel.MEMORY_ONLY)`.
`cashe()` не принимает аргументы, в то время как `persist()` может принимать в качестве аргумента уровень хранения:
- `StorageLevel.MEMORY_AND_DISK` (Параметр по умолчанию. Хранит материализованные объекты Row в куче памяти и диске/локальной памяти экзекутора, если в памяти закончится место)
- `StorageLevel.DISK_ONLY` (Хранит материализованные объекты Row только на диске/локальной памяти экзекутора)
- `StorageLevel.MEMORY_ONLY` (Хранит материализованные объекты Row только в куче памяти экзекутора)
- `StorageLevel.MEMORY_AND_DISK_SER` (Работает как MEMORY_AND_DISK, только сериализует объекты)
- `StorageLevel.MEMORY_ONLY_SER`
- `StorageLevel.DISK_ONLY_SER`
- и некоторые другие уровни выходящие за рамки рассматриваемой проблемы.

Гипотеза
---
`cache` крайне ограничен в сравнении с `persist` из-за возможности кэшировать объекты только в куче памяти экзекутора. Поэтому, попробуем сделать вывод, рассмотрев распределение объектов в куче памяти экзекутора:
![](/img/heap.jpg)
У каждого экзекутора имеется выделенная память которая разделена на 2 зоны:
- Память для кэшированных объектов.
- Память для объектов, создающихся в процессе расчётов задач в Spark.

Теперь стоит проговорить важное правило. Память для вычислений всегда имеет право использовать память для кэширования. Даже если память кэширования заполнена.
Поэтому, применительно к `cache()`, есть вероятность, что часть кешированных данных может быть вытеснена жизненно необходимыми объектами для экзекутора.
Но, в целом, мы знаем, что вышеуказанные методы кэширования - это отказоустойчивые методы. Благодаря тому, что сохраняется data-lineage. Поэтому экзекутор всегда может восстановить недостающие данные запустив повторный расчёт по data-lineage. А повторный расчёт, это то чего мы избегаем в нашей таске.
Это удтверждение не проверено и не доказано, но возможно стоит проверить в рамках нашего кейса.

checkpoint()
------
Сушествуют альтернативный способ кэширования DataFrame.
```scala
DataFrame.checkpoint(eager=True)
```
В рамках таски попробуем применить данный вид кэширования.  `checkpoint()` может принимать в качестве аргумента флажок `true` (no lazy chackpoint) или `false` (lazy checkpoint). По умолчанию - `true`.

Между *persist* и *checkpoint* есть существенная разница. *persist* материализует DataFrame и сохраняет (десериализованные объекты) его в памяти и/или на диске. Но data-lineage будет сохранена, так что в случае сбоя узлов и потери части кэшированных данных их можно будет восстановить. Однако *checkpoint()* сохраняет DataFrame в файл на HDFS и полностью стирает data-lineage. Это позволяет обрезать длинные цепочки путей вычислений и надежно сохранять данные в HDFS, которая, естественно, является отказоустойчивой при репликации.

Попытки применить *checkpoint* совместно с *persist* не привели к оптимальным результатам. Неуместное использование persist или checkpoint может лишь увеличить времязатратность расчётов. В рамках исследования было принято решение заменить persist на checkpoint:

```scala
val persistedSourceDf = sourceDf.persist
val persistedSourceDf = sourceDf.checkpoint
```
В таблице отражены результаты времязатратности вычислений, при использовании persist и checkpoint:  

| Расчёт №  | persist()  | checkpoint()  | chechpoint(false)  | localCheckpoint()  | localCheckpoint(false) |
| :------------: | :------------: | :------------: | :------------: | :------------: | :------------:|
| 1  |  88,27 s. | 76,05 s.   | 44,63 s.   | 52,43 s.  |  45,19 s. |
|  2 |  91,90 s. |  70,20 s.  |  44,07 s.  | 43,84 s.  |  49,32 s. |
|  3 |  89,31 s. | 74,80  s. | 48,33 s.   | 44,12 s.   |  48,75 s. |
|  4 |  89,93 s. | 71,18  s. | 48,33 s.   |  54,22 s.  |  46,58 s. |
|  5 |  88,99 s. |  70,66 s. |  47,83 s.  |  48,35 s.  |  49,27 s. |



