/*
 * Copyright (c) 2021 IBA Group, a.s. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package by.iba.vf.spark.transformation.stage.write

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.{ClickhouseStageConfig, Stage}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.sql.{Connection, DriverManager, Statement}

class ClickhouseWriteStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar{
  val id: String = "id"
  val host: String = "hst"
  val port: String = "prt"
  val database: String = "test"
  val user: String = "user"
  val password: String = "pwd"

  val configs: List[Map[String, String]] = List(
    Map(
      "operation" -> "WRITE",
      "storage" -> "clickhouse",
      "host" -> host,
      "port" -> port,
      "database" -> database,
      "user" -> user,
      "password" -> password,
      "schema" -> "shm",
      "table" -> "tbl",
      "writeMode" -> "Append",
      "driver" -> "com.clickhouse.jdbc.ClickHouseDriver"
    ),
    Map(
      "operation" -> "WRITE",
      "storage" -> "clickhouse",
      "host" -> host,
      "port" -> port,
      "database" -> database,
      "user" -> user,
      "password" -> password,
      "schema" -> "shm",
      "table" -> "tbl",
      "writeMode" -> "Append",
      "driver" -> "com.clickhouse.jdbc.ClickHouseDriver"
    )
  )

  it("write") {
      implicit val spark: SparkSession = mock[SparkSession]

      val dfWriter = mock[DataFrameWriter[Row]]
      val df = mock[DataFrame]

      for (config <- configs) {
        val conf: ClickhouseStageConfig = new ClickhouseStageConfig(Node(id, config))

        if (conf.write_mode.orNull.equals("Overwrite")){
          truncate(conf.table.orNull, conf.parameter("url"), conf.user, conf.password)

          when(df.write).thenReturn(dfWriter)
          when(dfWriter.format("jdbc")).thenReturn(dfWriter)
          when(dfWriter.options(conf.parameter)).thenReturn(dfWriter)
          when(dfWriter.mode("append")).thenReturn(dfWriter)
          doNothing.when(dfWriter).save
        } else if (conf.write_mode.orNull.equals("Append")) {
          when(df.write).thenReturn(dfWriter)
          when(dfWriter.format("jdbc")).thenReturn(dfWriter)
          when(dfWriter.options(conf.parameter)).thenReturn(dfWriter)
          when(dfWriter.mode("append")).thenReturn(dfWriter)
          doNothing.when(dfWriter).save
        }

        new ClickhouseWriteStage(id, conf).write(df)
      }
  }

  def truncate(tableName: String, jdbcUrl: String, username: String, password: String): Boolean = {
    val connection = mock[Connection]
    val statement = mock[Statement]

    when(DriverManager.getConnection(jdbcUrl, username, password)).thenReturn(connection)
    doNothing.when(connection.setAutoCommit(true))
    when(connection.createStatement()).thenReturn(statement)
    when(statement.execute(s"TRUNCATE TABLE $tableName")).thenReturn(true)
  }
}

class ClickhouseWriteStageBuilderTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  val id: String = "id"
  val host: String = "hst"
  val port: String = "prt"
  val database: String = "test"
  val user: String = "user"
  val password: String = "pwd"

  val configs: List[Map[String, String]] = List(
    Map(
      "operation" -> "WRITE",
      "storage" -> "clickhouse",
      "host" -> host,
      "port" -> port,
      "database" -> database,
      "user" -> user,
      "password" -> password,
      "schema" -> "shm",
      "table" -> "tbl",
      "writeMode" -> "Append",
      "driver" -> "com.clickhouse.jdbc.ClickHouseDriver"
    ),
    Map(
      "operation" -> "WRITE",
      "storage" -> "clickhouse",
      "host" -> host,
      "port" -> port,
      "database" -> database,
      "user" -> user,
      "password" -> password,
      "schema" -> "shm",
      "table" -> "tbl",
      "writeMode" -> "Overwrite",
      "driver" -> "com.clickhouse.jdbc.ClickHouseDriver"
    )
  )

  it("validate") {
    for (config <- configs) {
      val result = ClickhouseWriteStageBuilder invokePrivate PrivateMethod[Boolean](Symbol("validate"))(config)
      result should be(true)
    }
  }

  it("convert") {
    for (config <- configs) {
      val result = ClickhouseWriteStageBuilder invokePrivate PrivateMethod[Stage](Symbol("convert"))(Node(id, config))
      result.getClass should be(classOf[ClickhouseWriteStage])
    }
  }
}
