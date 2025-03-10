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
import by.iba.vf.spark.transformation.stage.config.objectstores.COSConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec

class ObjectStorageWriteStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  it("ObjectStorageWriteStage") {
    val cosConfig = mock[COSConfig]
    val df = mock[DataFrame]
    val dfWriter = mock[DataFrameWriter[Row]]
    val spark = mock[SparkSession]
    val stage = new ObjectStorageWriteStage(Node("id1", Map()), ObjectStorageWriteCOSStageBuilder, cosConfig, Map(), "cos_storage")

    when(dfWriter.options(Map[String, String]())).thenReturn(dfWriter)
    when(cosConfig.saveMode).thenReturn(Some("overwrite"))
    when(cosConfig.format).thenReturn(Some("csv"))
    when(cosConfig.connectPath).thenReturn("cp")
    doNothing.when(cosConfig).setConfig(spark)
    when(cosConfig.addPartitions(dfWriter)).thenReturn(dfWriter)

    when(df.write).thenReturn(dfWriter)
    when(dfWriter.mode("overwrite")).thenReturn(dfWriter)
    when(dfWriter.format("csv")).thenReturn(dfWriter)
    doNothing.when(dfWriter).save("cp")

    stage.write(df)(spark)
  }

  it("S3WriteStage") {
    val cosConfig = mock[COSConfig]
    val df = mock[DataFrame]
    val dfWriter = mock[DataFrameWriter[Row]]
    val spark = mock[SparkSession]
    val optionsMap = Map("a" -> "b", "b" -> "c")
    val stage = new ObjectStorageWriteStage(Node("id1", Map()), ObjectStorageWriteS3StageBuilder, cosConfig, optionsMap, "s3_storage")

    when(dfWriter.options(optionsMap)).thenReturn(dfWriter)
    when(cosConfig.saveMode).thenReturn(None)
    when(cosConfig.format).thenReturn(Some("csv"))
    when(cosConfig.connectPath).thenReturn("cp")
    doNothing.when(cosConfig).setConfig(spark)
    when(cosConfig.addPartitions(dfWriter)).thenReturn(dfWriter)

    when(df.write).thenReturn(dfWriter)
    when(dfWriter.format("csv")).thenReturn(dfWriter)
    doNothing.when(dfWriter).save("cp")

    stage.write(df)(spark)
  }

}
