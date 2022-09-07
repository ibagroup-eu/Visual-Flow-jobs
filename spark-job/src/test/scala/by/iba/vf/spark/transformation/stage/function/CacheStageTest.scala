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
package by.iba.vf.spark.transformation.stage.function

import by.iba.vf.spark.transformation.config.Node
import by.iba.vf.spark.transformation.stage.OperationType
import by.iba.vf.spark.transformation.stage.Stage
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class CacheStageTest extends AnyFunSpec with MockitoSugar with PrivateMethodTester {
  it("process") {
    implicit lazy val spark: SparkSession = mock[SparkSession]
    val df = mock[DataFrame]
    val storageLevel = mock[StorageLevel]
    when(df.persist(storageLevel)).thenReturn(df)
    val stage = new CacheStage("id", storageLevel)

    val result = stage invokePrivate PrivateMethod[Option[DataFrame]]('process)(Map("1" -> df), spark)

    result should be(Some(df))
  }

  it("cache") {
    val df = mock[DataFrame]
    val storageLevel = mock[StorageLevel]
    when(df.persist(storageLevel)).thenReturn(df)

    val stage = new CacheStage("id", storageLevel)
    val result = stage.cache(df)

    result should be(df)
  }
}

class CacheStageBuilderTest extends AnyFunSpec with PrivateMethodTester {
  val testMap = Map(
    "operation" -> OperationType.CACHE.toString,
    "useDisk" -> "true",
    "useMemory" -> "false",
    "useOffHeap" -> "true",
    "deserialized" -> "false",
    "replication" -> "3"
  )
  it("validate") {
    val config: Map[String, String] = testMap

    val result = CacheStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
    result should be(true)
  }

  it("convert") {
    val config: Node = Node("id", testMap)

    val result = CacheStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)

    result.getClass should be(classOf[CacheStage])
  }
}
