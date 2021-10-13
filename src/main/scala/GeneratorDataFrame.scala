import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.util.Random

class GeneratorDataFrame {

  /*
  val name = Array("Вася", "Петя", "Никита", "Аня", "Миша" , "Саша", "Таня", "Борис", "Ахмед", "Брюс",
                    "Тим", "Илья", "Дартаньян", "Шпрот", "Мрот", "Крот", "Марат", "Умалат", "Зина", "Лариса")

  val lastName = Array("Сидоров", "Меченый", "Ряженый", "Калеченный", "Поддатый", "Иванова", "Иванов", "Красный",
                        "Путин", "Чайковских", "Бражелончук", "Прокопчук", "Зеленских", "Рыбова", "Чуточких", "Ким")

   */
  val role = Array("младший", "средний", "старший", "охранник", "не охранник", "вахтёр", "аналитик", "программист",
  "водитель", "кондуктор", "инструктор", "кинолог", "археолог", "юрист", "адвокат", "маргинал", "пенсионер", "спортсмен",
  "аквамен", "супермен", "тракторист", "машинист", "геодезист", "повар", "специалист", "руководитель", "менеджер")

  val schema = StructType(Array(
    StructField("Name",StringType,true),
    StructField("Role",StringType,true),
    StructField("Experience",IntegerType,true),
    StructField("Born", IntegerType, true),
    StructField("Salary", IntegerType, true)
  ))

  def getDataFrame(spark: SparkSession, recordCount:Int) = {

    val data = for ( _ <- 0 to recordCount)
      yield Row(getName, getRole, getExperience, getBorn, getSalary)

    spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
  }

  def getName: String = Random.nextString(25)
  def getRole: String = role(Random.nextInt(role.length))
  def getExperience: Int = Random.nextInt(40)
  def getBorn: Int = 1910 + Random.nextInt(90)
  def getSalary: Int = 10000 + Random.nextInt(100000)

}
