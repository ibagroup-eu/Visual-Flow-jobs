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
import by.iba.vf.spark.transformation.stage.{RequestStageConfig, Stage}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, DatasetHolder, Encoders, SQLContext, SparkSession}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.{be, convertToAnyShouldWrapper}
import requests.Response

class RequestReadStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  it("read") {
    implicit val encoder = Encoders.STRING
    implicit val spark = mock[SparkSession]
    implicit val sqlContext: SQLContext = mock[SQLContext]

    val id: String = "id"
    val host: String = "https://api.restful-api.dev/objects/7"
    val method: String = "get"
    val optionParams: String = "a:b;"
    val optionHeaders: String = "c:d;"

    val config = new RequestStageConfig(
      Node(
        id,
        Map(
          "operation" -> "READ",
          "storage" -> "request",
          "method" -> method,
          "host" -> host
        )
      )
    )

    val options = Map(
      "option.params" -> optionParams,
      "option.headers" -> optionHeaders,
    )

    val dfReader = mock[DataFrameReader]
    val df = mock[DataFrame]
    val implicits = mock[spark.implicits.type]
    val response = mock[Response]
    val textResponse: String = """{"id":"7","name":"Apple MacBook Pro 16","data":{"year":2019,"price":1849.99,"CPU model":"Intel Core i9","Hard disk size":"1 TB"}}"""
    val dsResponse = mock[Dataset[String]]
    val dshResponse = mock[DatasetHolder[String]]

    when(spark.implicits).thenReturn(implicits)
    when(implicits.newStringEncoder).thenReturn(Encoders.STRING)

    when(spark.read).thenReturn(dfReader)
    when(dfReader.option("multiline","true")).thenReturn(dfReader)
    when(response.text()).thenReturn(textResponse)
    when(implicits.localSeqToDatasetHolder(Seq(response.text()))).thenReturn(dshResponse)
    when(dshResponse.toDS()).thenReturn(dsResponse)
    when(dfReader.json(dsResponse)).thenReturn(df)

    val stage = new RequestReadStage(Node(id, Map()), config, options)
    stage.read
  }
}

class RequestReadStageBuilderTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {

  val id: String = "id"
  val host: String = "https"
  val method: String = "get"

  val conf: Map[String, String] = Map(
    "operation" -> "READ",
    "storage" -> "request",
    "method" -> method,
    "host" -> host
  )

  it("validate") {
    val result = RequestReadStageBuilder invokePrivate PrivateMethod[Boolean](Symbol("validate"))(conf)
    result should be(true)
  }

  it("convert") {
    val result = RequestReadStageBuilder invokePrivate PrivateMethod[Stage](Symbol("convert"))(Node(id, conf))
    result.getClass should be(classOf[RequestReadStage])
  }
}

