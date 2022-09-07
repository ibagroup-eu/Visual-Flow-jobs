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
package by.iba.vf.spark.transformation.stage

import by.iba.vf.spark.transformation.config.Node
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.SparkSession
import org.mockito.scalatest.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class ObjectStorageStageTest extends AnyFunSpec with PrivateMethodTester with MockitoSugar {
  val m: Map[String, String] = Map(
    "accessKey" -> "ak",
    "bucket" -> "bt",
    "format" -> "fmt",
    "path" -> "pth",
    "writeMode" -> "wm",
    "secretKey" -> "sk"
  )
  val cosNode = Node("id1", m + ("endpoint" -> "ep") + ("storage" -> "cos") + ("authType" -> "HMAC"))
  val s3Node = Node("id1", m + ("storage" -> "s3") + ("anonymousAccess" -> "false") + ("endpoint" -> "ep") + ("ssl" -> "false"))

  describe("COSConfig") {

    it("applyConfig") {
      val conf = new COSConfig(cosNode)
      val spark = mock[SparkSession]
      val runtimeConf = mock[RuntimeConfig]
      when(spark.conf).thenReturn(runtimeConf)
      doNothing.when(runtimeConf).set(any[String], any[String])

      conf.setConfig(spark)

      verify(runtimeConf).set("fs.stocator.scheme.list", "cos")
      verify(runtimeConf).set(s"fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
      verify(runtimeConf).set(s"fs.stocator.cos.impl", s"com.ibm.stocator.fs.cos.COSAPIClient")
      verify(runtimeConf).set(s"fs.stocator.cos.scheme", "cos")
      verify(runtimeConf).set(s"fs.cos.service.endpoint", "ep")
      verify(runtimeConf).set(s"fs.cos.service.access.key", "ak")
      verify(runtimeConf).set(s"fs.cos.service.secret.key", "sk")
    }
    it("path") {
      new COSConfig(cosNode).connectPath should be ("cos://bt.service/pth")
    }
    it("validate") {
      COSConfig.validate(cosNode.value) should be (true)
      COSConfig.validate(s3Node.value) should be (false)
    }
  }

  describe("S3Config") {

    it("applyConfig") {
      val conf = new S3Config(s3Node)
      val spark = mock[SparkSession]
      val runtimeConf = mock[RuntimeConfig]
      when(spark.conf).thenReturn(runtimeConf)
      doNothing.when(runtimeConf).set(any[String], any[String])

      conf.setConfig(spark)
      verify(runtimeConf).set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      verify(runtimeConf).set("fs.s3a.access.key", "ak")
      verify(runtimeConf).set("fs.s3a.secret.key", "sk")
    }
    it("path") {
      new S3Config(s3Node).connectPath should be ("s3a://bt/pth")
    }
    it("validate") {
      S3Config.validate(cosNode.value) should be (false)
      S3Config.validate(s3Node.value) should be (true)
    }

  }
}