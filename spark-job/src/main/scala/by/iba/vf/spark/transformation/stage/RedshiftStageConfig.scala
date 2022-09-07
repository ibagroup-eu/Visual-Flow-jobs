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
import by.iba.vf.spark.transformation.stage.RedshiftStageConfig._
import org.apache.spark.SparkContext

class RedshiftStageConfig(config: Node) {
  val redshiftHost: String = config.value(host)
  val redshiftPort: String = config.value(port)
  val redshiftDatabase: String = config.value(database)
  val redshiftSsl: String = config.value(ssl)
  val redshiftUser: String = config.value(user)
  val redshiftPassword: String = config.value(password)
  val redshiftTable: Option[String] = config.value.get(table)
  val redshiftQuery: Option[String] = config.value.get(query)
  val redshiftCustomSql: Boolean = config.value.get(customSql).exists(x => x.toBoolean)
  val s3Bucket: String = config.value(bucket)
  val accessKey: String = config.value(s3AccessKey)
  val secretKey: String = config.value(s3SecretKey)
  val copyOptions: Option[String] = config.value.get(extraCopyOptions)
  val saveMode: Option[String] = config.value.get(writeModeFieldName)

  def setUpConfigParams(sc: SparkContext): Unit = {
    sc.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    sc.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
  }

  def provideConnectionOptions: Map[String, String] = {
    var configMap: Map[String, String] = Map(
      "url" -> s"jdbc:redshift://$redshiftHost:$redshiftPort/$redshiftDatabase?ssl=$redshiftSsl",
      "tempdir" -> s3Bucket,
      "user" -> redshiftUser,
      "password" -> redshiftPassword,
      "forward_spark_s3_credentials" -> "true",
    )
    copyOptions match {
      case Some(copyOpts) => configMap += ("extracopyoptions" -> copyOpts)
      case _ =>
    }
    configMap
  }
}

object RedshiftStageConfig {
  val storageId = "redshift"
  val host = "host"
  val port = "port"
  val ssl = "ssl"
  val database = "database"
  val user = "user"
  val password = "password"
  val customSql = "customSql"
  val table = "table"
  val dbtable = "dbtable"
  val query = "query"
  val bucket = "bucket"
  val s3AccessKey = "accessKey"
  val s3SecretKey = "secretKey"
  val extraCopyOptions = "extraCopyOptions"
  val writeModeFieldName = "writeMode"

  private val requiredParameters = List(
    host,
    port,
    ssl,
    database,
    user,
    password,
    bucket,
    s3AccessKey,
    s3SecretKey,
  )

  def validateConfig(config: Map[String, String]): Boolean = {
    val customSqlMode = config.get(customSql).exists(x => x.toBoolean)
    requiredParameters.forall(param =>
      config.contains(param)) &&
      ((customSqlMode && config.contains(query)) || (!customSqlMode && config.contains(database))) &&
      config(bucket).startsWith("s3a")
  }
}
