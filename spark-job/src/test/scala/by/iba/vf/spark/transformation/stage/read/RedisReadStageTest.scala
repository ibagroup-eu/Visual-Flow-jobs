package by.iba.vf.spark.transformation.stage.read

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.{RedisStageConfig, Stage}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext, SparkSession}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class RedisReadStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  it("read") {
    implicit val spark: SparkSession = mock[SparkSession]
    implicit val sqlContext: SQLContext = mock[SQLContext]

    val id: String = "id"
    val table: String = "tbl"
    val host: String = "hst"
    val port: String = "prt"
    val password: String = "pwd"

    val config = new RedisStageConfig(Node(id, Map("storage" -> "redis", "table" -> table, "host" -> host,  "password" -> password, "port" -> port, "ssl" -> "false", "readMode" -> "key", "operation" -> "READ")))

    val dfReader = mock[DataFrameReader]
    val df = mock[DataFrame]
    when(spark.read).thenReturn(dfReader)
    when(dfReader.format("org.apache.spark.sql.redis")).thenReturn(dfReader)
    when(dfReader.options(Map("table" -> table, "host" -> host,  "auth" -> password, "port" -> port, "ssl" -> "false"))).thenReturn(dfReader)
    when(dfReader.load).thenReturn(df)

    val stage = new RedisReadStage(Node(id, Map()), config)
    stage.read
  }
}

class RedisReadStageBuilderTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {

  val id: String = "id"
  val table: String = "tbl"
  val host: String = "hst"
  val port: String = "prt"
  val password: String = "pwd"
  val ssl: String = "false"

  val conf: Map[String, String] = Map("storage" -> "redis", "table" -> table, "host" -> host, "password" -> password, "port" -> port, "ssl" -> "false", "readMode" -> "key", "operation" -> "READ")

  it("validate") {
    val result = RedisReadStageBuilder invokePrivate PrivateMethod[Boolean](Symbol("validate"))(conf)
    result should be(true)
  }

  it("convert") {
    val result = RedisReadStageBuilder invokePrivate PrivateMethod[Stage](Symbol("convert"))(Node(id, conf))
    result.getClass should be(classOf[RedisReadStage])
  }
}
