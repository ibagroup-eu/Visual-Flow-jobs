package by.iba.vf.spark.transformation.stage.read

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.{CassandraStageConfig, Stage}
import org.apache.spark.sql.cassandra.DataFrameReaderWrapper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{DataFrame, DataFrameReader, RuntimeConfig, SQLContext, SparkSession}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class CassandraReadStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  it("read") {
    implicit val spark: SparkSession = mock[SparkSession]
    implicit val sqlContext: SQLContext = mock[SQLContext]

    val id: String       = "id"
    val table: String    = "tbl"
    val keyspace: String    = "kspc"
    val host: String = "hst"
    val port: String     = "prt"
    val password: String = "pwd"
    val username: String = "un"

    val config = new CassandraStageConfig(Node(id, Map("storage" -> "cassandra", "table" -> table, "keyspace" -> keyspace, "host" -> host, "username" -> username, "password" -> password, "port" -> port, "ssl" -> "false", "pushdownEnabled" -> "true", "operation"-> "READ")))

    val dfReader = mock[DataFrameReader]
    val df = mock[DataFrame]
    when(spark.read).thenReturn(dfReader)
    val configClass = classOf[RuntimeConfig]
    val ctor = configClass.getDeclaredConstructor(classOf[SQLConf])
    ctor.setAccessible(true)
    val runtimeConfig = ctor.newInstance(new SQLConf)
    when(spark.conf).thenReturn(runtimeConfig)
    when(dfReader.format("org.apache.spark.sql.cassandra")).thenReturn(dfReader)
    when(dfReader.options(Map("cluster" -> "", "keyspace" -> keyspace, "table" -> table, "pushdown" -> "true"))).thenReturn(dfReader)
    when(dfReader.cassandraFormat(table, keyspace, "", pushdownEnable = true)).thenReturn(dfReader)
    when(dfReader.load).thenReturn(df)

    val stage = new CassandraReadStage(Node(id, Map()), config)
    stage.read
  }
}

class CassandraReadStageBuilderTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {

  val id: String       = "id"
  val table: String    = "tbl"
  val keyspace: String    = "kspc"
  val host: String    = "hst"
  val port: String     = "prt"
  val password: String = "pwd"
  val username: String = "un"
  val ssl: String = "false"

  val conf: Map[String, String] = Map("storage" -> "cassandra", "table" -> table, "keyspace" -> keyspace, "host" -> host, "username" -> username, "password" -> password, "port" -> port, "ssl" -> "false", "pushdownEnabled" -> "true", "operation"-> "READ")

  it("validate") {
    val result = CassandraReadStageBuilder invokePrivate PrivateMethod[Boolean](Symbol("validate"))(conf)
    result should be(true)
  }

  it("convert") {
    val result = CassandraReadStageBuilder invokePrivate PrivateMethod[Stage](Symbol("convert"))(Node(id, conf))
    result.getClass should be(classOf[CassandraReadStage])
  }
}
