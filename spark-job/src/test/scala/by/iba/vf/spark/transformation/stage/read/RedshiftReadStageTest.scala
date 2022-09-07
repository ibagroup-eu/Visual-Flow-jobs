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
import by.iba.vf.spark.transformation.stage.{RedshiftStageConfig, Stage}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext, SparkSession}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class RedshiftReadStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  it("read") {
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
          "operation" -> "READ"
        )
      )
    )

    val dfReader = mock[DataFrameReader]
    val df = mock[DataFrame]
    when(spark.read).thenReturn(dfReader)
    when(dfReader.format("io.github.spark_redshift_community.spark.redshift")).thenReturn(dfReader)
    when(dfReader.options(
      Map(
        "url" -> s"jdbc:redshift://$host:$port/$database?ssl=false",
        "tempdir" -> tempdir,
        "user" -> user,
        "password" -> password,
        "forward_spark_s3_credentials" -> "true",
        "dbtable" -> table
      )
    )).thenReturn(dfReader)
    when(spark.sparkContext).thenReturn(sc)
    when(sc.hadoopConfiguration).thenReturn(hadoopConfig)
    when(dfReader.load).thenReturn(df)
    doNothing.when(hadoopConfig).set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    doNothing.when(hadoopConfig).set("fs.s3a.access.key", accessKey)
    doNothing.when(hadoopConfig).set("fs.s3a.secret.key", secretKey)

    val stage = new RedshiftReadStage(id, config)
    stage.read
  }
}

class RedshiftReadStageBuilderTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {

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
    "operation" -> "READ"
  )

  it("validate") {
    val result = RedshiftReadStageBuilder invokePrivate PrivateMethod[Boolean](Symbol("validate"))(conf)
    result should be(true)
  }

  it("convert") {
    val result = RedshiftReadStageBuilder invokePrivate PrivateMethod[Stage](Symbol("convert"))(Node(id, conf))
    result.getClass should be(classOf[RedshiftReadStage])
  }
}
