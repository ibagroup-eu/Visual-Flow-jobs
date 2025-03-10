package by.iba.vf.spark.transformation.stage.write

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.{KafkaStageConfig, Stage}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class KafkaWriteStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  val conf: Map[String, String] = Map(
    "bootstrapServers" -> "bs",
    "topic" -> "tp",
  )

  val option: Map[String, String] = Map(
    "option.kafka.security.protocol" -> "SASL_SSL",
    "option.kafka.ssl.truststore.location" -> "kafka.truststore.jks",
    "option.kafka.ssl.truststore.password" -> "changeit",
    "option.kafka.sasl.mechanism" -> "PLAIN",
    "option.kafka.ssl.endpoint.identification.algorithm" -> ""
  )

  it("write") {
    implicit val spark: SparkSession = mock[SparkSession]

    val dfWriter = mock[DataFrameWriter[Row]]
    val df = mock[DataFrame]

    val config = new KafkaStageConfig(Node("id", conf))

    when(df.write).thenReturn(dfWriter)
    when(dfWriter.format("kafka")).thenReturn(dfWriter)
    when(dfWriter.option("kafka.bootstrap.servers", conf("bootstrapServers"))).thenReturn(dfWriter)
    when(dfWriter.option("topic", conf("topic"))).thenReturn(dfWriter)
    when(dfWriter.options(option)).thenReturn(dfWriter)
    doNothing.when(dfWriter).save


    new KafkaWriteStage(Node("id", Map()), config, option).write(df)
  }
}

class KafkaWriteStageBuilderTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  val bootstrapServers: String = "bs"
  val topic: String = "tp"

  val conf = Map(
    "storage" -> "kafka",
    "bootstrapServers" -> bootstrapServers,
    "topic" -> topic,
    "operation" -> "WRITE"
  )

  it("validate") {
    val result = KafkaWriteStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(conf)
    result should be(true)
  }

  it("convert") {
    val config: Node = Node("id", conf)
    val result = KafkaWriteStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)
    result.getClass should be(classOf[KafkaWriteStage])
  }
}