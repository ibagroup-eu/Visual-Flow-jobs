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
import by.iba.vf.spark.transformation.stage.{RedshiftStageConfig, Stage}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SQLContext, SparkSession}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class RedshiftWriteStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  it("write") {
    implicit val spark: SparkSession = mock[SparkSession]
    implicit val sqlContext: SQLContext = mock[SQLContext]
    implicit val sc: SparkContext = mock[SparkContext]
    implicit val hadoopConfig: Configuration = mock[Configuration]

    val id: String = "id"
    val table: String = "tbl"
    val host: String = "hst"
    val port: String = "prt"
    val password: String = "pwd"
    val user: String = "user"
    val tempdir: String = "s3a://test-bckt"
    val accessKey: String = "test"
    val secretKey: String = "test"
    val database: String = "test"
    val saveMode: String = "Overwrite"

    val config = new RedshiftStageConfig(
      Node(
        id,
        Map(
          "storage" -> "redshift",
          "table" -> table,
          "host" -> host,
          "user" -> user,
          "database" -> database,
          "password" -> password,
          "port" -> port,
          "ssl" -> "false",
          "customSql" -> "false",
          "bucket" -> tempdir,
          "accessKey" -> accessKey,
          "secretKey" -> secretKey,
          "operation" -> "WRITE",
          "writeMode"-> saveMode,
        )
      )
    )

    val dfWriter = mock[DataFrameWriter[Row]]
    val df = mock[DataFrame]
    when(spark.sparkContext).thenReturn(sc)
    when(sc.hadoopConfiguration).thenReturn(hadoopConfig)
    when(df.write).thenReturn(dfWriter)
    when(dfWriter.format("io.github.spark_redshift_community.spark.redshift")).thenReturn(dfWriter)
    when(dfWriter.options(
      Map(
        "url" -> s"jdbc:redshift://$host:$port/$database?ssl=false",
        "tempdir" -> tempdir,
        "user" -> user,
        "password" -> password,
        "forward_spark_s3_credentials" -> "true",
        "dbtable" -> table
      )
    )).thenReturn(dfWriter)
    when(dfWriter.mode(saveMode)).thenReturn(dfWriter)
    doNothing.when(hadoopConfig).set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    doNothing.when(hadoopConfig).set("fs.s3a.access.key", accessKey)
    doNothing.when(hadoopConfig).set("fs.s3a.secret.key", secretKey)
    doNothing.when(dfWriter).save
    new RedshiftWriteStage(id, config).write(df)
  }
}

class RedshiftWriteStageBuilderTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {

  val id: String = "id"
  val table: String = "tbl"
  val host: String = "hst"
  val port: String = "prt"
  val password: String = "pwd"
  val user: String = "user"
  val tempdir: String = "s3a://test-bckt"
  val accessKey: String = "test"
  val secretKey: String = "test"
  val database: String = "test"

  val conf: Map[String, String] = Map(
    "storage" -> "redshift",
    "table" -> table,
    "host" -> host,
    "user" -> user,
    "database" -> database,
    "password" -> password,
    "port" -> port,
    "ssl" -> "false",
    "customSql" -> "false",
    "bucket" -> tempdir,
    "accessKey" -> accessKey,
    "secretKey" -> secretKey,
    "writeMode"-> "Overwrite",
    "operation" -> "WRITE"
  )

  it("validate") {
    val result = RedshiftWriteStageBuilder invokePrivate PrivateMethod[Boolean](Symbol("validate"))(conf)
    result should be(true)
  }

  it("convert") {
    val result = RedshiftWriteStageBuilder invokePrivate PrivateMethod[Stage](Symbol("convert"))(Node(id, conf))
    result.getClass should be(classOf[RedshiftWriteStage])
  }
}
