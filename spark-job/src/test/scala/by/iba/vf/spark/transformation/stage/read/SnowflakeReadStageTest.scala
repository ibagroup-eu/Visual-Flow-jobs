package by.iba.vf.spark.transformation.stage.read

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.{SnowflakeStageConfig, Stage}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class SnowflakeReadStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  val conf: Map[String, String] = Map(
    "sfURL" -> "url",
    "sfUser" -> "user",
    "sfPassword" -> "password",
    "sfDatabase" -> "database",
    "sfSchema" -> "schema",
    "sfWarehouse" -> "warehouse",
    "sfRole" -> "role",
    "dbtable" -> "dbtable"
  )

  it("read") {
    implicit val spark: SparkSession = mock[SparkSession]

    val dfReader = mock[DataFrameReader]
    val df = mock[DataFrame]

    val config = new SnowflakeStageConfig(Node("id", conf))

    when(spark.read).thenReturn(dfReader)
    when(dfReader.format("net.snowflake.spark.snowflake")).thenReturn(dfReader)
    when(dfReader.options(config.parameter)).thenReturn(dfReader)
    when(dfReader.load()).thenReturn(df)

    new SnowflakeReadStage(Node("id", Map()), config).read should be(df)
  }
}

class SnowflakeReadStageBuilderTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  val sfURL: String = "url"
  val sfUser: String = "user"
  val sfPassword: String = "password"
  val sfDatabase: String = "database"
  val sfSchema: String = "schema"
  val dbtable: String = "dbtable"

  val conf = Map(
    "storage" -> "snowflake",
    "sfURL" -> sfURL,
    "sfUser" -> sfUser,
    "sfPassword" -> sfPassword,
    "sfDatabase" -> sfDatabase,
    "sfSchema" -> sfSchema,
    "dbtable" -> dbtable,
    "operation" -> "READ"
  )

  it("validate") {
    val result = SnowflakeReadStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(conf)
    result should be(true)
  }

  it("convert") {
    val config: Node = Node("id", conf)
    val result = SnowflakeReadStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)
    result.getClass should be(classOf[SnowflakeReadStage])
  }
}
