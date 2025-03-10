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
package by.iba.vf.spark.transformation.stage.read

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.{ClickhouseStageConfig, Stage}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class ClickhouseReadStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar{
  val id: String = "id"
  val host: String = "hst"
  val port: String = "prt"
  val database: String = "test"
  val user: String = "user"
  val password: String = "pwd"

  val configs: List[Map[String, String]] = List(
    Map(
      "operation" -> "READ",
      "storage" -> "clickhouse",
      "host" -> host,
      "port" -> port,
      "database" -> database,
      "user" -> user,
      "password" -> password,
      "customSql" -> "false",
      "schema" -> "shm",
      "table" -> "tbl",
      "driver" -> "com.clickhouse.jdbc.ClickHouseDriver"
    ),
    Map(
      "operation" -> "READ",
      "storage" -> "clickhouse",
      "host" -> host,
      "port" -> port,
      "database" -> database,
      "user" -> user,
      "password" -> password,
      "customSql" -> "true",
      "query" -> "qry",
      "driver" -> "com.clickhouse.jdbc.ClickHouseDriver"
    )
  )

  it("read") {
    implicit val spark: SparkSession = mock[SparkSession]

    val dfReader = mock[DataFrameReader]
    val df = mock[DataFrame]

    for (config <- configs) {
      val conf: ClickhouseStageConfig = new ClickhouseStageConfig(Node(id, config))

      when(spark.read).thenReturn(dfReader)
      when(dfReader.format("jdbc")).thenReturn(dfReader)
      when(dfReader.options(conf.parameter)).thenReturn(dfReader)
      when(dfReader.load()).thenReturn(df)

      val stage = new ClickhouseReadStage(Node(id, Map()), conf)
      val result = stage.read

      result should be(df)
    }

  }
}

class ClickhouseReadStageBuilderTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  val id: String = "id"
  val host: String = "hst"
  val port: String = "prt"
  val database: String = "test"
  val user: String = "user"
  val password: String = "pwd"

  val configs: List[Map[String, String]] = List(
    Map(
      "operation" -> "READ",
      "storage" -> "clickhouse",
      "host" -> host,
      "port" -> port,
      "database" -> database,
      "user" -> user,
      "password" -> password,
      "customSql" -> "false",
      "schema" -> "shm",
      "table" -> "tbl",
      "driver" -> "com.clickhouse.jdbc.ClickHouseDriver"
    ),
    Map(
      "operation" -> "READ",
      "storage" -> "clickhouse",
      "host" -> host,
      "port" -> port,
      "database" -> database,
      "user" -> user,
      "password" -> password,
      "customSql" -> "true",
      "query" -> "qry",
      "driver" -> "com.clickhouse.jdbc.ClickHouseDriver"
    )
  )

  it("validate") {
    for (config <- configs) {
      val result = ClickhouseReadStageBuilder invokePrivate PrivateMethod[Boolean](Symbol("validate"))(config)
      result should be(true)
    }
  }

  it("convert") {
    for (config <- configs) {
      val result = ClickhouseReadStageBuilder invokePrivate PrivateMethod[Stage](Symbol("convert"))(Node(id, config))
      result.getClass should be(classOf[ClickhouseReadStage])
    }
  }
}
