package by.iba.vf.spark.transformation.stage.function

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.{OperationType, Stage}
import org.apache.spark.sql.functions.{add_months, col, current_date, current_timestamp, date_add, date_format, date_sub, date_trunc, datediff, dayofmonth, dayofweek, dayofyear, from_unixtime, hour, last_day, minute, month, months_between, next_day, quarter, second, to_date, to_timestamp, trunc, unix_timestamp, weekofyear, year}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class DateTimeStageTest extends AnyFunSpec with MockitoSugar with PrivateMethodTester {
  val configs: List[Map[String, String]] = List(
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "current_date",
      "option.targetColumn" -> "current_date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "date_format",
      "option.sourceColumn" -> "current_date",
      "option.format" -> "format",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "to_date",
      "option.sourceColumn" -> "current_date",
      "option.format" -> "MM/dd/yyyy",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "add_months",
      "option.sourceColumn" -> "current_date",
      "option.numMonths" -> "3",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "date_add",
      "option.sourceColumn" -> "current_date",
      "option.days" -> "4",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "date_sub",
      "option.sourceColumn" -> "current_date",
      "option.days" -> "5",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "datediff",
      "option.endColumn" -> "end_current_date",
      "option.startColumn" -> "start_current_date",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "months_between",
      "option.endColumn" -> "end_current_date",
      "option.startColumn" -> "start_current_date",
      "option.roundOff" -> "true",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "next_day",
      "option.sourceColumn" -> "current_date",
      "option.dayOfWeek" -> "Sunday",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "year",
      "option.sourceColumn" -> "current_date",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "quarter",
      "option.sourceColumn" -> "current_date",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "month",
      "option.sourceColumn" -> "current_date",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "dayofweek",
      "option.sourceColumn" -> "current_date",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "dayofyear",
      "option.sourceColumn" -> "current_date",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "weekofyear",
      "option.sourceColumn" -> "current_date",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "last_day",
      "option.sourceColumn" -> "current_date",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "trunc",
      "option.sourceColumn" -> "current_timestamp",
      "option.format" -> "year",
      "option.targetColumn" -> "timestamp"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "current_timestamp",
      "option.targetColumn" -> "current_timestamp"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "hour",
      "option.sourceColumn" -> "current_timestamp",
      "option.targetColumn" -> "timestamp"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "minute",
      "option.sourceColumn" -> "current_timestamp",
      "option.targetColumn" -> "timestamp"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "second",
      "option.sourceColumn" -> "current_timestamp",
      "option.targetColumn" -> "timestamp"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "to_timestamp",
      "option.sourceColumn" -> "current",
      "option.format" -> "yyyy-MM-dd HH:mm:ss",
      "option.targetColumn" -> "current_timestamp"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "date_trunc",
      "option.sourceColumn" -> "date",
      "option.format" -> "day",
      "option.targetColumn" -> "current_date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "unix_timestamp",
      "option.targetColumn" -> "current_unix_timestamp"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "to_unix_timestamp",
      "option.sourceColumn" -> "unix_timestamp",
      "option.format" -> "yyyy-MM-dd HH:mm:ss",
      "option.targetColumn" -> "current_unix_timestamp"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "from_unixtime",
      "option.sourceColumn" -> "unix_timestamp",
      "option.format" -> "yyyy-MM-dd HH:mm:ss",
      "option.targetColumn" -> "current_unix_timestamp"
    )
  )

  private def getOptions(config: Map[String, String]): Map[String, String] = {
    val optionPattern = "option\\.(.+)".r
    config
      .flatMap {
        case (optionPattern(optionName), optionValue) => Some(optionName -> optionValue)
        case _ => None
      }
  }

  it("process") {
    implicit lazy val spark: SparkSession = mock[SparkSession]
    val df = mock[DataFrame]
    val df2 = mock[DataFrame]

    for (config <- configs) {
      if (config("function") == "current_date")
        when(df.withColumn(getOptions(config)("targetColumn"), current_date())).thenReturn(df2)
      else if (config("function") == "date_format")
        when(df.withColumn(getOptions(config)("targetColumn"), date_format(col(getOptions(config)("sourceColumn")),
          getOptions(config)("format")))).thenReturn(df2)
      else if (config("function") == "to_date")
        when(df.withColumn(getOptions(config)("targetColumn"), to_date(col(getOptions(config)("sourceColumn")),
          getOptions(config)("format")))).thenReturn(df2)
      else if (config("function") == "add_months")
        when(df.withColumn(getOptions(config)("targetColumn"), add_months(col(getOptions(config)("sourceColumn")),
          getOptions(config)("numMonths").toInt))).thenReturn(df2)
      else if (config("function") == "date_add")
        when(df.withColumn(getOptions(config)("targetColumn"), date_add(col(getOptions(config)("sourceColumn")),
          getOptions(config)("days").toInt))).thenReturn(df2)
      else if (config("function") == "date_sub")
        when(df.withColumn(getOptions(config)("targetColumn"), date_sub(col(getOptions(config)("sourceColumn")),
          getOptions(config)("days").toInt))).thenReturn(df2)
      else if (config("function") == "datediff")
        when(df.withColumn(getOptions(config)("targetColumn"), datediff(col(getOptions(config)("endColumn")),
          col(getOptions(config)("startColumn"))))).thenReturn(df2)
      else if (config("function") == "months_between")
        when(df.withColumn(getOptions(config)("targetColumn"), months_between(col(getOptions(config)("endColumn")),
          col(getOptions(config)("startColumn")), getOptions(config)("roundOff").toBoolean))).thenReturn(df2)
      else if (config("function") == "next_day")
        when(df.withColumn(getOptions(config)("targetColumn"), next_day(col(getOptions(config)("sourceColumn")),
          getOptions(config)("dayOfWeek")))).thenReturn(df2)
      else if (config("function") == "year")
        when(df.withColumn(getOptions(config)("targetColumn"), year(col(getOptions(config)("sourceColumn")))))
          .thenReturn(df2)
      else if (config("function") == "quarter")
        when(df.withColumn(getOptions(config)("targetColumn"), quarter(col(getOptions(config)("sourceColumn")))))
          .thenReturn(df2)
      else if (config("function") == "month")
        when(df.withColumn(getOptions(config)("targetColumn"), month(col(getOptions(config)("sourceColumn")))))
          .thenReturn(df2)
      else if (config("function") == "dayofweek")
        when(df.withColumn(getOptions(config)("targetColumn"), dayofweek(col(getOptions(config)("sourceColumn")))))
          .thenReturn(df2)
      else if (config("function") == "dayofmonth")
        when(df.withColumn(getOptions(config)("targetColumn"), dayofmonth(col(getOptions(config)("sourceColumn")))))
          .thenReturn(df2)
      else if (config("function") == "dayofyear")
        when(df.withColumn(getOptions(config)("targetColumn"), dayofyear(col(getOptions(config)("sourceColumn")))))
          .thenReturn(df2)
      else if (config("function") == "weekofyear")
        when(df.withColumn(getOptions(config)("targetColumn"), weekofyear(col(getOptions(config)("sourceColumn")))))
          .thenReturn(df2)
      else if (config("function") == "last_day")
        when(df.withColumn(getOptions(config)("targetColumn"), last_day(col(getOptions(config)("sourceColumn")))))
          .thenReturn(df2)
      else if (config("function") == "trunc")
        when(df.withColumn(getOptions(config)("targetColumn"), trunc(col(getOptions(config)("sourceColumn")),
          getOptions(config)("format")))).thenReturn(df2)
      else if (config("function") == "current_timestamp")
        when(df.withColumn(getOptions(config)("targetColumn"), current_timestamp())).thenReturn(df2)
      else if (config("function") == "hour")
        when(df.withColumn(getOptions(config)("targetColumn"), hour(col(getOptions(config)("sourceColumn")))))
          .thenReturn(df2)
      else if (config("function") == "minute")
        when(df.withColumn(getOptions(config)("targetColumn"), minute(col(getOptions(config)("sourceColumn")))))
          .thenReturn(df2)
      else if (config("function") == "second")
        when(df.withColumn(getOptions(config)("targetColumn"), second(col(getOptions(config)("sourceColumn")))))
          .thenReturn(df2)
      else if (config("function") == "to_timestamp")
        when(df.withColumn(getOptions(config)("targetColumn"), to_timestamp(col(getOptions(config)("sourceColumn")),
          getOptions(config)("format")))).thenReturn(df2)
      else if (config("function") == "date_trunc")
        when(df.withColumn(getOptions(config)("targetColumn"), date_trunc(getOptions(config)("format"),
          col(getOptions(config)("sourceColumn"))))).thenReturn(df2)
      else if (config("function") == "unix_timestamp")
        when(df.withColumn(getOptions(config)("targetColumn"), unix_timestamp())).thenReturn(df2)
      else if (config("function") == "to_unix_timestamp")
        when(df.withColumn(getOptions(config)("targetColumn"), unix_timestamp(col(getOptions(config)("sourceColumn")),
          getOptions(config)("format")))).thenReturn(df2)
      else if (config("function") == "from_unixtime")
        when(df.withColumn(getOptions(config)("targetColumn"), from_unixtime(col(getOptions(config)("sourceColumn")),
          getOptions(config)("format")))).thenReturn(df2)
      val stage = new DateTimeStage(Node("id", Map()), config("function"), getOptions(config))
      val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> df), spark)
      result should be(Some(df2))
    }

  }
}
class DateTimeStageBuilderTest extends AnyFunSpec with PrivateMethodTester {
  val configs: List[Map[String, String]] = List(
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "current_date",
      "option.targetColumn" -> "current_date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "date_format",
      "option.sourceColumn" -> "current_date",
      "option.format" -> "format",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "to_date",
      "option.sourceColumn" -> "current_date",
      "option.format" -> "MM/dd/yyyy",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "add_months",
      "option.sourceColumn" -> "current_date",
      "option.numMonths" -> "3",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "date_add",
      "option.sourceColumn" -> "current_date",
      "option.days" -> "4",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "date_sub",
      "option.sourceColumn" -> "current_date",
      "option.days" -> "5",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "datediff",
      "option.endColumn" -> "end_current_date",
      "option.startColumn" -> "start_current_date",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "months_between",
      "option.endColumn" -> "end_current_date",
      "option.startColumn" -> "start_current_date",
      "option.roundOff" -> "true",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "next_day",
      "option.sourceColumn" -> "current_date",
      "option.dayOfWeek" -> "Sunday",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "year",
      "option.sourceColumn" -> "current_date",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "quarter",
      "option.sourceColumn" -> "current_date",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "month",
      "option.sourceColumn" -> "current_date",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "dayofweek",
      "option.sourceColumn" -> "current_date",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "dayofyear",
      "option.sourceColumn" -> "current_date",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "weekofyear",
      "option.sourceColumn" -> "current_date",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "last_day",
      "option.sourceColumn" -> "current_date",
      "option.targetColumn" -> "date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "trunc",
      "option.sourceColumn" -> "current_timestamp",
      "option.format" -> "year",
      "option.targetColumn" -> "timestamp"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "current_timestamp",
      "option.targetColumn" -> "current_timestamp"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "hour",
      "option.sourceColumn" -> "current_timestamp",
      "option.targetColumn" -> "timestamp"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "minute",
      "option.sourceColumn" -> "current_timestamp",
      "option.targetColumn" -> "timestamp"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "second",
      "option.sourceColumn" -> "current_timestamp",
      "option.targetColumn" -> "timestamp"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "to_timestamp",
      "option.sourceColumn" -> "current",
      "option.format" -> "yyyy-MM-dd HH:mm:ss",
      "option.targetColumn" -> "current_timestamp"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "date_trunc",
      "option.sourceColumn" -> "date",
      "option.format" -> "day",
      "option.targetColumn" -> "current_date"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "unix_timestamp",
      "option.targetColumn" -> "current_unix_timestamp"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "to_unix_timestamp",
      "option.sourceColumn" -> "unix_timestamp",
      "option.format" -> "yyyy-MM-dd HH:mm:ss",
      "option.targetColumn" -> "current_unix_timestamp"
    ),
    Map(
      "operation" -> OperationType.DATETIME.toString,
      "function" -> "from_unixtime",
      "option.sourceColumn" -> "unix_timestamp",
      "option.format" -> "yyyy-MM-dd HH:mm:ss",
      "option.targetColumn" -> "current_unix_timestamp"
    )
  )

  it("validate") {
    for (config <- configs) {
      val result = DateTimeStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
      result should be(true)
    }
  }

  it("convert") {
    for (config <- configs) {
      val nodeConfig = Node("id", config)
      val result = DateTimeStageBuilder invokePrivate PrivateMethod[Stage]('convert)(nodeConfig)
      result.getClass should be(classOf[DateTimeStage])
    }
  }
}
