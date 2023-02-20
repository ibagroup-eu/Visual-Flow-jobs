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
import by.iba.vf.spark.transformation.stage.{DataframeStageConfig, Stage}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class DataframeReadStageTest extends AnyFunSpec with MockitoSugar {
  val id = "table1"
  val conf: Map[String, String] = Map(
    "storage" -> "dataframe",
    "operation"-> "READ",
    "data"-> "[[\"0\",\"Ulad\"],[\"1\",\"Roman\"]]",
    "schema"-> "[{\"column\":\"id\",\"type\":\"String\"},{\"column\":\"name\",\"type\":\"String\"}]",
  )

  it("read") {
    implicit val spark: SparkSession = mock[SparkSession]
    val context = mock[SparkContext]

    val config = new DataframeStageConfig(Node(id, conf))

    val stage = new DataframeReadStage(id, config)

    val df = mock[DataFrame]
    val rdd = mock[RDD[Row]]

    val schema = stage.createSparkSchema(config.parseSchema())
    val data = stage.typeCasting(config.parseData(), config.parseSchema())

    when(spark.sparkContext).thenReturn(context)
    when(context.defaultParallelism).thenReturn(1)
    when(context.makeRDD(data)).thenReturn(rdd)
    when(spark.createDataFrame(rdd, schema)).thenReturn(df)

    val result = stage.read
    result should be(df)
  }
}

class DataframeReadStageBuilderTest extends AnyFunSpec with PrivateMethodTester {
  val id = "table1"
  val conf: Map[String, String] = Map(
    "storage" -> "dataframe",
    "operation"-> "READ",
    "data"-> "[[\"0\",\"Ulad\"],[\"1\",\"Roman\"]]",
    "schema"-> "[{\"column\":\"id\",\"type\":\"String\"},{\"column\":\"name\",\"type\":\"String\"}]",
  )

  it("validate") {
    val result = DataframeReadStageBuilder invokePrivate PrivateMethod[Boolean](Symbol("validate"))(conf)
    result should be(true)
  }

  it("convert") {
    val result = DataframeReadStageBuilder invokePrivate PrivateMethod[Stage](Symbol("convert"))(Node(id, conf))
    result.getClass should be(classOf[DataframeReadStage])
  }
}
