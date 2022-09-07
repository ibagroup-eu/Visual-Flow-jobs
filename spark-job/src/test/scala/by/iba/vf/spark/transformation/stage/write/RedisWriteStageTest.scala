package by.iba.vf.spark.transformation.stage.write

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.{RedisStageConfig, Stage}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class RedisWriteStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  it("write") {
    implicit val spark: SparkSession = mock[SparkSession]

    val id: String = "id"
    val table: String = "tbl"
    val host: String = "hst"
    val port: String = "prt"
    val password: String = "pwd"
    val saveMode: String = "Append"

    val config = new RedisStageConfig(Node(id, Map("storage" -> "redis", "table" -> table, "host" -> host,  "password" -> password, "port" -> port, "ssl" -> "false", "writeMode" -> saveMode, "operation" -> "WRITE")))

    val dfWriter = mock[DataFrameWriter[Row]]
    val df = mock[DataFrame]
    when(df.write).thenReturn(dfWriter)
    when(dfWriter.format("org.apache.spark.sql.redis")).thenReturn(dfWriter)
    when(dfWriter.options(Map("table" -> table, "host" -> host,  "auth" -> password, "port" -> port, "ssl" -> "false"))).thenReturn(dfWriter)
    when(dfWriter.mode(saveMode)).thenReturn(dfWriter)
    doNothing.when(dfWriter).save
    new RedisWriteStage(id, config).write(df)
  }
}

class RedisWriteStageBuilderTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {

  val id: String = "id"
  val table: String = "tbl"
  val host: String = "hst"
  val port: String = "prt"
  val password: String = "pwd"
  val ssl: String = "false"

  val conf: Map[String, String] = Map("storage" -> "redis", "table" -> table, "host" -> host, "password" -> password, "port" -> port, "ssl" -> "false", "operation" -> "WRITE", "writeMode" -> "Append")

  it("validate") {
    val result = RedisWriteStageBuilder invokePrivate PrivateMethod[Boolean](Symbol("validate"))(conf)
    result should be(true)
  }

  it("convert") {
    val result = RedisWriteStageBuilder invokePrivate PrivateMethod[Stage](Symbol("convert"))(Node(id, conf))
    result.getClass should be(classOf[RedisWriteStage])
  }
}
