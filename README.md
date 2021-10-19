# ResearchDataFrame

В рамках библиотеки *ML360_DM_common* рассмотрена возможность использования функции `checkpoint()` в методе `writeFeatureTable(...)`

#### Таска NBACB-3244:
При тестировании витрины u_calendar_holidays замечено, что в `saveAsStgTable` дважды происходит повторный расчёт DataFrame: `count` и `saveAsTable`. Вероятно это происходит при `autoRepartition`.

Как запустить проект?
--------
- Открыть проект, дождаться загрузки зависимостей
- Выполнить *Run* sbt-проекта

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
После первого запуска в корневом каталоге проекта сохранится таблица в формате parquet. Затем, если нет необходимости в методе, следует закомментировать его.

Класс GeneratorDataFrame будет генерировать DataFrame со следующими полями:
```scala
val schema = StructType(Array(
    StructField("name_last_name",StringType,true),
    StructField("role",StringType,true),
    StructField("experience",IntegerType,true),
    StructField("born", IntegerType, true),
    StructField("salary", IntegerType, true)
  ))
```  


#### - cache()
#### - persist()
#### - checkpoint()

![](/img/df-line.jpg)

| Расчёт №  | persist()  | checkpoint()  | chechpoint(false)  | localCheckpoint()  | localCheckpoint(false) |
| :------------: | :------------: | :------------: | :------------: | :------------: | :------------:|
| 1  |  88,27 s. | 76,05 s.   | 44,63 s.   | 52,43 s.  |  45,19 s. |
|  2 |  91,90 s. |  70,20 s.  |  44,07 s.  | 43,84 s.  |  49,32 s. |
|  3 |  89,31 s. | 74,80  s. | 48,33 s.   | 44,12 s.   |  48,75 s. |
|  4 |  89,93 s. | 71,18  s. | 48,33 s.   |  54,22 s.  |  46,58 s. |
|  5 |  88,99 s. |  70,66 s. |  47,83 s.  |  48,35 s.  |  49,27 s. |


Между кешем и контрольной точкой есть существенная разница. Кэш материализует RDD и сохраняет его в памяти (и / или на диске). Но происхождение (вычислительная цепочка) RDD (то есть последовательность операций, которые сгенерировали RDD) будет запомнено, так что в случае сбоев узлов и потери части кэшированных RDD их можно будет восстановить. Однако контрольная точка сохраняет RDD в файл HDFS и фактически полностью забывает родословную. Это позволяет обрезать длинные родословные и надежно сохранять данные в HDFS, которая, естественно, является отказоустойчивой при репликации.

https://github.com/JerryLead/SparkInternals/blob/master/markdown/english/6-CacheAndCheckpoint.md


