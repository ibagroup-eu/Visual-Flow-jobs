package by.iba.vf.spark.transformation.stage.write

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.{SnowflakeStageConfig, Stage}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession, SaveMode}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class SnowflakeWriteStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  val conf: Map[String, String] = Map(
    "sfURL" -> "url",
    "sfUser" -> "user",
    "sfPassword" -> "password",
    "sfDatabase" -> "database",
    "sfSchema" -> "schema",
    "sfWarehouse" -> "warehouse",
    "sfRole" -> "role",
    "query" -> "query"
  )

  it("write") {
    implicit val spark: SparkSession = mock[SparkSession]

    val dfWriter = mock[DataFrameWriter[Row]]
    val df = mock[DataFrame]

    val config = new SnowflakeStageConfig(Node("id", conf))

    when(df.write).thenReturn(dfWriter)
    when(dfWriter.format("net.snowflake.spark.snowflake")).thenReturn(dfWriter)
    when(dfWriter.options(config.parameter)).thenReturn(dfWriter)
    when(dfWriter.mode(SaveMode.Overwrite)).thenReturn(dfWriter)
    doNothing.when(dfWriter).save
    new SnowflakeWriteStage(Node("id", Map()), config).write(df)
  }
}

class SnowflakeWriteStageBuilderTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  val sfURL: String = "url"
  val sfUser: String = "user"
  val sfPassword: String = "password"
  val sfDatabase: String = "database"
  val sfSchema: String = "schema"
  val query: String = "query"

  val conf = Map(
    "storage" -> "snowflake",
    "sfURL" -> sfURL,
    "sfUser" -> sfUser,
    "sfPassword" -> sfPassword,
    "sfDatabase" -> sfDatabase,
    "sfSchema" -> sfSchema,
    "query" -> query,
    "operation" -> "WRITE"
  )

  it("validate") {
    val result = SnowflakeWriteStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(conf)
    result should be(true)
  }

  it("convert") {
    val config: Node = Node("id", conf)
    val result = SnowflakeWriteStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)
    result.getClass should be(classOf[SnowflakeWriteStage])
  }
}
