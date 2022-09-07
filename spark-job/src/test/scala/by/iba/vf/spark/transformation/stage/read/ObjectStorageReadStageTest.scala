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

import by.iba.vf.spark.transformation.stage.COSConfig
import by.iba.vf.spark.transformation.stage.S3Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.SparkSession
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class ObjectStorageReadStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  it("ObjectStorageReadStage") {
    val cosConfig = mock[COSConfig]
    val df = mock[DataFrame]
    val dfReader = mock[DataFrameReader]
    val spark = mock[SparkSession]
    val optionsMap = Map("a" -> "b", "b" -> "c")
    val stage = new ObjectStorageReadStage("id1", ObjectStorageReadCOSStageBuilder, cosConfig, optionsMap, "cos_storage")

    when(dfReader.options(optionsMap)).thenReturn(dfReader)
    when(cosConfig.format).thenReturn("csv")
    when(cosConfig.connectPath).thenReturn("cp")
    doNothing.when(cosConfig).setConfig(spark)

    when(spark.read).thenReturn(dfReader)
    when(dfReader.format("csv")).thenReturn(dfReader)
    when(dfReader.load("cp")).thenReturn(df)

    val result = stage.read(spark)

    result should be (df)
  }

  it("S3ReadStage") {
    val s3Config = mock[S3Config]
    val df = mock[DataFrame]
    val dfReader = mock[DataFrameReader]
    val spark = mock[SparkSession]
    val optionsMap = Map("a" -> "b", "b" -> "c")
    val stage = new ObjectStorageReadStage("id1", ObjectStorageReadS3StageBuilder, s3Config, optionsMap, "cos_storage")

    when(dfReader.options(optionsMap)).thenReturn(dfReader)
    when(s3Config.format).thenReturn("csv")
    when(s3Config.connectPath).thenReturn("cp")
    doNothing.when(s3Config).setConfig(spark)

    when(spark.read).thenReturn(dfReader)
    when(dfReader.format("csv")).thenReturn(dfReader)
    when(dfReader.load("cp")).thenReturn(df)

    val result = stage.read(spark)

    result should be (df)
  }

}
