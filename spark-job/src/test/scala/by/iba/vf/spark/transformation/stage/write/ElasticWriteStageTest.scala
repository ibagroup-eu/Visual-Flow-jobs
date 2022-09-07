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
import by.iba.vf.spark.transformation.stage.Stage
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql._
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class ElasticWriteStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  it("write") {
    implicit val spark: SparkSession = mock[SparkSession]

    val id: String       = "id"
    val index: String    = "idx"
    val nodes: String    = "nds"
    val port: String     = "prt"
    val password: String = "pwd"
    val username: String = "un"

    val config = Map("es.nodes" -> nodes,
      "es.port" -> port,
      "es.net.http.auth.user" -> username,
      "es.net.http.auth.pass" -> password,
      "es.net.ssl" -> "true",
      "es.nodes.wan.only" -> "true")

    val dfWriter = mock[DataFrameWriter[Row]]
    val df = mock[DataFrame]
    when(df.write).thenReturn(dfWriter)
    when(dfWriter.format("org.elasticsearch.spark.sql")).thenReturn(dfWriter)
    when(dfWriter.options(config)).thenReturn(dfWriter)
    doNothing.when(dfWriter).save("idx")

    val stage = new ElasticWriteStage(id, index, None, None, config)
    stage.write(df)
  }
}

class ElasticWriteStageBuilderTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {

  val id: String       = "id"
  val index: String    = "idx"
  val nodes: String    = "nds"
  val port: String     = "prt"
  val password: String = "pwd"
  val username: String = "un"
  val ssl: String = "true"

  val conf = Map(
    "storage" -> "elastic",
    "index" -> index,
    "nodes" -> nodes,
    "port" -> port,
    "user" -> username,
    "password" -> password,
    "ssl" -> ssl,
    "operation"-> "WRITE"
  )

  it("validate") {
    val result = ElasticWriteStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(conf)
    result should be(true)
  }

  it("convert") {
    val config: Node = Node("id", conf)
    val result = ElasticWriteStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)
    result.getClass should be(classOf[ElasticWriteStage])
  }
}
