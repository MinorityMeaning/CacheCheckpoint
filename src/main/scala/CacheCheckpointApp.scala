import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CacheCheckpointApp extends App {

  val cachingMode = List("NO_CACHE_NO_CHECKPOINT", "CACHE", "PERSIST" , "CHECKPOINT", "CHECKPOINT_NON_EAGER")

  val spark = SparkSession.builder()
              .appName("Testing cache and checkpoint")
              .master("local[*]")
              .getOrCreate()

  spark.sparkContext.setCheckpointDir("/tmp/checkpoints")

  val recordCount = 100000 // Количество строк для генерируемой таблицы

  val df = new GeneratorDataFrame().getDataFrame(spark, recordCount)
  df.show()

  val resultTime = cachingMode.map(processDataFrame)

  for((mode, time) <- cachingMode zip resultTime)
    println(s"$mode - $time mls")


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////



  def processDataFrame(mode: String): Long = {
    val t0 = System.currentTimeMillis
    val topDf = df.filter(col("Salary").>(50000))

    val cacheDf = mode match {
          case "CACHE" => topDf.cache()
          case "PERSIST" => topDf.persist()
          case "CHECKPOINT" => topDf.checkpoint()
          case "CHECKPOINT_NON_EAGER" => topDf.checkpoint(false)
          case _ => topDf
          }

    val roleList = cacheDf.groupBy("Role")
                          .count()
                          .orderBy("Role")
                          .collect()
    val bornList = cacheDf.groupBy("Born")
                          .count()
                          .orderBy(col("Born").desc)
                          .collect()

    val t1 = System.currentTimeMillis()

    println("Количество сотрудников на должностях")
    roleList.foreach(arr => println(s"${arr(0)} ${arr(1)}"))
    println("Распределение сотрудников по году рождения")
    bornList.foreach(arr => println(s"${arr(0)} ${arr(1)}"))

    t1-t0   // Затраченное на расчёты время
  }

}
