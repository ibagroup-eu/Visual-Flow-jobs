package by.iba.vf.spark.transformation.stage.read

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.write.KafkaWriteStage
import by.iba.vf.spark.transformation.stage.{KafkaStageConfig, Stage}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, SparkSession}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class KafkaReadStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  val conf: Map[String, String] = Map(
    "bootstrapServers" -> "bs",
    "subscribe" -> "sb",
  )

  val option: Map[String, String] = Map(
    "option.kafka.security.protocol" -> "SASL_SSL",
    "option.kafka.ssl.truststore.location" -> "kafka.truststore.jks",
    "option.kafka.ssl.truststore.password" -> "changeit",
    "option.kafka.sasl.mechanism" -> "PLAIN",
    "option.kafka.ssl.endpoint.identification.algorithm" -> ""
  )

  it("read") {
    implicit val spark: SparkSession = mock[SparkSession]

    val dfReader = mock[DataFrameReader]
    val df = mock[DataFrame]

    val config = new KafkaStageConfig(Node("id", conf))

    when(spark.read).thenReturn(dfReader)
    when(dfReader.format("kafka")).thenReturn(dfReader)
    when(dfReader.option("kafka.bootstrap.servers", conf("bootstrapServers"))).thenReturn(dfReader)
    when(dfReader.option("subscribe", conf("subscribe"))).thenReturn(dfReader)
    when(dfReader.option("startingOffsets", "earliest")).thenReturn(dfReader)
    when(dfReader.options(option)).thenReturn(dfReader)
    when(dfReader.load()).thenReturn(df)

    new KafkaReadStage(Node("id", Map()), config, option).read
  }
}

class KafkaReadStageBuilderTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  val bootstrapServers: String = "bs"
  val subscribe: String = "sb"

  val conf = Map(
    "storage" -> "kafka",
    "bootstrapServers" -> bootstrapServers,
    "subscribe" -> subscribe,
    "operation" -> "READ"
  )

  it("validate") {
    val result = KafkaReadStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(conf)
    result should be(true)
  }

  it("convert") {
    val config: Node = Node("id", conf)
    val result = KafkaReadStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)
    result.getClass should be(classOf[KafkaReadStage])
  }
}