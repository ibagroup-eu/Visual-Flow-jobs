package by.iba.vf.spark.transformation.stage.write

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.{CassandraStageConfig, Stage}
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.internal.SQLConf
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class CassandraWriteStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  it("write") {
    implicit val spark: SparkSession = mock[SparkSession]

    val id: String = "id"
    val table: String = "tbl"
    val keyspace: String = "kspc"
    val host: String = "hst"
    val port: String = "prt"
    val password: String = "pwd"
    val username: String = "un"
    val saveMode: String = "Append"

    val config = new CassandraStageConfig(Node(id, Map("storage" -> "cassandra", "table" -> table, "keyspace" -> keyspace, "host" -> host, "username" -> username, "password" -> password, "port" -> port, "ssl" -> "false", "writeMode" -> saveMode, "pushdownEnabled" -> "false", "operation"-> "WRITE")))

    val dfWriter = mock[DataFrameWriter[Row]]
    val df = mock[DataFrame]
    val configClass = classOf[RuntimeConfig]
    val ctor = configClass.getDeclaredConstructor(classOf[SQLConf])
    ctor.setAccessible(true)
    val runtimeConfig = ctor.newInstance(new SQLConf)
    when(dfWriter.mode(saveMode)).thenReturn(dfWriter)
    when(spark.conf).thenReturn(runtimeConfig)
    when(df.write).thenReturn(dfWriter)
    when(dfWriter.format("org.apache.spark.sql.cassandra")).thenReturn(dfWriter)
    when(dfWriter.cassandraFormat(table, keyspace, "", pushdownEnable = false)).thenReturn(dfWriter)
    doNothing.when(dfWriter).save

    val stage = new CassandraWriteStage(id, config)
    stage.write(df)
  }
}

class CassandraWriteStageBuilderTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {

  val id: String = "id"
  val table: String = "tbl"
  val keyspace: String = "kspc"
  val host: String = "hst"
  val port: String = "prt"
  val password: String = "pwd"
  val username: String = "un"
  val ssl: String = "false"
  val saveMode: String = "Append"

  val conf: Map[String, String] = Map("storage" -> "cassandra", "table" -> table, "keyspace" -> keyspace, "host" -> host, "username" -> username, "password" -> password, "port" -> port, "ssl" -> "false", "writeMode" -> saveMode, "pushdownEnabled" -> "true", "operation"-> "WRITE")

  it("validate") {
    val result = CassandraWriteStageBuilder invokePrivate PrivateMethod[Boolean](Symbol("validate"))(conf)
    result should be(true)
  }

  it("convert") {
    val result = CassandraWriteStageBuilder invokePrivate PrivateMethod[Stage](Symbol("convert"))(Node(id, conf))
    result.getClass should be(classOf[CassandraWriteStage])
  }
}
