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
import by.iba.vf.spark.transformation.stage.OperationType
import by.iba.vf.spark.transformation.stage.Stage
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.noException
import org.scalatest.matchers.should.Matchers._

class StdoutWriteStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  it("process") {
    implicit val spark: SparkSession = mock[SparkSession]
    val df = mock[DataFrame]
    doNothing.when(df).show(10, truncate = false)
    when(df.count()).thenReturn(1)

    val stage = new StdoutWriteStage(Node("id", Map()), 10)

    noException should be thrownBy stage.write(df)
  }
}

class StdoutWriteStageBuilderTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {

  it("validate") {
    val config: Map[String, String] =
      Map("operation" -> OperationType.WRITE.toString, "storage" -> StdoutWriteStageBuilder.StdoutStorage)
    val result = StdoutWriteStageBuilder invokePrivate PrivateMethod[Boolean]('validate)(config)
    result should be(true)
  }

  it("convert") {
    val config: Node = Node("id", Map("storage" -> OperationType.WRITE.toString, "quantity" -> "20"))

    val result = StdoutWriteStageBuilder invokePrivate PrivateMethod[Stage]('convert)(config)

    result.getClass should be(classOf[StdoutWriteStage])
  }
}
