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
import by.iba.vf.spark.transformation.stage.Stage
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class DataGeneratorStageTest extends AnyFunSpec with MockitoSugar {
// TODO: make this test work
//  it("read") {
//    implicit val spark: SparkSession = mock[SparkSession]
//    when(spark.implicits).thenReturn()
//    val stage = new DataGeneratorStage("id", "alias")
//    val result = stage.read
//  }
}

class DataGeneratorStageBuilderTest extends AnyFunSpec with PrivateMethodTester {
  it("validate") {
    val config: Map[String, String] = Map("operation" -> "READ",
      "storage" -> DataGeneratorStageBuilder.GeneratorStorage, "alias" -> "test")
    val result = DataGeneratorStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
    result should be(true)
  }

  it("convert") {
    val config: Node = Node("id", Map("storage" -> DataGeneratorStageBuilder.GeneratorStorage, "alias" -> "test"))

    val result = DataGeneratorStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)

    result.getClass should be(classOf[DataGeneratorStage])
  }
}
